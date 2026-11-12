//! Verdicts of the server scenarios: pure functions from what the public listener answered to checks.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

use gang_wire::{
    CLOSED_EVENT, Closed, ClosedReason, Detail, ERROR_DOMAIN, ErrorReason, ErrorResponse,
    SNAPSHOT_EVENT,
};
use serde_json::Value;

use crate::check::Check;
use crate::identity::ChannelEvent;
use crate::names;
use crate::server::{
    Answer, CERTIFICATE_BACKDATE, CERTIFICATE_TTL, Exit, Handshake, Leaf, REISSUE_AT_PERCENT,
    VIEWER_RETRY_AFTER,
};

/// How long after the reissue instant a leaf's `notBefore` implies the next connection is made: one
/// second for the whole-second truncation of `notBefore`, one for the reissue to land.
pub const REISSUE_SLACK: Duration = Duration::from_secs(2);

/// Passes when `answer` is the refusal `reason` names: its status, the AIP-193 envelope around
/// it, one `ErrorInfo` with the gang domain, the reason's name, exactly the reason's metadata keys
/// and each of `metadata`'s values.
///
/// # Errors
///
/// The first rule the answer breaks, quoting what was observed.
pub fn refusal(
    answer: &Answer,
    reason: ErrorReason,
    metadata: &[(&str, &str)],
) -> std::result::Result<(), String> {
    let expected = reason.http();
    if answer.status != expected {
        return Err(format!(
            "answered {}, not {expected}: {}",
            answer.status,
            answer.text()
        ));
    }
    let response = serde_json::from_slice::<ErrorResponse>(&answer.body).map_err(|error| {
        format!(
            "the body is not an error envelope ({}): {}",
            gang_types::error_chain(&error),
            answer.text()
        )
    })?;
    let error = &response.error;
    if error.code != expected {
        return Err(format!(
            "the envelope's code is {}, not {expected}",
            error.code
        ));
    }
    if error.status != reason.status() {
        return Err(format!(
            "the envelope's status is {:?}, not {:?}",
            error.status,
            reason.status()
        ));
    }
    let infos = error_infos(&error.details);
    let [info] = infos.as_slice() else {
        return Err(format!("{} ErrorInfo details, not 1", infos.len()));
    };
    if info.domain != ERROR_DOMAIN {
        return Err(format!(
            "the ErrorInfo domain is {:?}, not {ERROR_DOMAIN:?}",
            info.domain
        ));
    }
    if info.reason != reason.as_str() {
        return Err(format!(
            "the ErrorInfo reason is {:?}, not {:?}",
            info.reason,
            reason.as_str()
        ));
    }
    let keys: BTreeSet<&str> = info.metadata.keys().map(String::as_str).collect();
    let expected_keys: BTreeSet<&str> = reason.metadata_keys().iter().copied().collect();
    if keys != expected_keys {
        return Err(format!(
            "the ErrorInfo metadata keys are {keys:?}, not {expected_keys:?}"
        ));
    }
    for &(key, value) in metadata {
        if info.metadata.get(key).map(String::as_str) != Some(value) {
            return Err(format!(
                "the ErrorInfo metadata {key} is {:?}, not {value:?}",
                info.metadata.get(key)
            ));
        }
    }
    Ok(())
}

/// Passes when [`refusal`] is Ok for `answer`; a failure names `line`, the request made.
pub fn refused(
    name: &'static str,
    line: &str,
    answer: &Answer,
    reason: ErrorReason,
    metadata: &[(&str, &str)],
) -> Check {
    match refusal(answer, reason, metadata) {
        Ok(()) => Check::passed(name),
        Err(why) => Check::failed(name, format!("{line}: {why}")),
    }
}

/// Passes when a request presenting no credentials is refused as `CREDENTIALS_MISSING` and
/// challenged with `WWW-Authenticate: Bearer`.
pub fn challenged(line: &str, answer: &Answer) -> Check {
    let name = names::api::UNAUTHENTICATED_CHALLENGED;
    if let Err(why) = refusal(answer, ErrorReason::CredentialsMissing, &[]) {
        return Check::failed(name, format!("{line}: {why}"));
    }
    match answer.header("www-authenticate") {
        Some("Bearer") => Check::passed(name),
        other => Check::failed(
            name,
            format!("{line}: WWW-Authenticate is {other:?}, not \"Bearer\""),
        ),
    }
}

/// Passes when every observed non-2xx `/v1` answer is a well-formed error envelope with exactly one
/// gang `ErrorInfo`; fails when none was observed.
pub fn envelopes(answers: &[(String, Answer)]) -> Check {
    let name = names::api::ENVELOPE_ON_EVERY_REFUSAL;
    if answers.is_empty() {
        return Check::failed(name, "no non-2xx /v1 answer was observed");
    }
    let mut failures = Vec::new();
    for (line, answer) in answers {
        for why in envelope_violations(answer) {
            failures.push(format!("{line}: {why}"));
        }
    }
    verdict(name, &failures)
}

/// One page of a list.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    /// The listed resources, in the order the server listed them.
    pub items: Vec<Value>,
    /// The `nextPageToken`, `None` when the answer carries none.
    pub next: Option<String>,
}

/// Reads a `200` answer as one page of the list in the array `field`.
///
/// # Errors
///
/// When the status is not 200, the body is not a JSON object, `field` is not an array, or
/// `nextPageToken` is present but not a string.
pub fn page(answer: &Answer, field: &str) -> std::result::Result<Page, String> {
    if answer.status != 200 {
        return Err(format!("answered {}: {}", answer.status, answer.text()));
    }
    let Ok(Value::Object(body)) = serde_json::from_slice::<Value>(&answer.body) else {
        return Err(format!("the body is not a JSON object: {}", answer.text()));
    };
    let Some(Value::Array(items)) = body.get(field) else {
        return Err(format!("no `{field}` array"));
    };
    let next = match body.get("nextPageToken") {
        None => None,
        Some(Value::String(token)) => Some(token.clone()),
        Some(other) => return Err(format!("nextPageToken is {other}, not a string")),
    };
    Ok(Page {
        items: items.clone(),
        next,
    })
}

/// Passes when a `pageSize` above the maximum lists `field` in one page: the answer is a page with
/// no `nextPageToken` and with items.
pub fn page_size_clamped(answer: &Answer, field: &str) -> Check {
    let name = names::api::PAGE_SIZE_CLAMPED;
    match page(answer, field) {
        Err(why) => Check::failed(name, why),
        Ok(Page {
            next: Some(token), ..
        }) => Check::failed(name, format!("the page carries nextPageToken {token}")),
        Ok(page) if page.items.is_empty() => {
            Check::failed(name, format!("the page lists no `{field}`"))
        }
        Ok(_) => Check::passed(name),
    }
}

/// Passes when the items of `walk`'s pages, concatenated, are exactly the items of `full`, and
/// `full` has at least two items so that paging is exercised.
pub fn walk_complete(name: &'static str, full: &Page, walk: &[Page]) -> Check {
    if full.items.len() < 2 {
        return Check::failed(
            name,
            format!(
                "paging needs at least two items; the full list has {}",
                full.items.len()
            ),
        );
    }
    let walked: Vec<&Value> = walk.iter().flat_map(|page| page.items.iter()).collect();
    if walked.len() == full.items.len() && walked.iter().zip(&full.items).all(|(a, b)| *a == b) {
        return Check::passed(name);
    }
    Check::failed(
        name,
        format!(
            "the walk lists {} items, the full list {}, and they differ in order or content",
            walked.len(),
            full.items.len()
        ),
    )
}

/// Passes when `walk` is non-empty, every page has items, and the last page carries no
/// `nextPageToken`.
pub fn last_page_omits_token(name: &'static str, walk: &[Page]) -> Check {
    if walk.is_empty() {
        return Check::failed(name, "no page was walked");
    }
    if let Some(index) = walk.iter().position(|page| page.items.is_empty()) {
        return Check::failed(name, format!("page {index} is empty"));
    }
    match walk.last() {
        Some(Page {
            next: Some(token), ..
        }) => Check::failed(name, format!("the last page carries nextPageToken {token}")),
        _ => Check::passed(name),
    }
}

/// The token with its first character replaced: `'A'` becomes `'B'`, anything else becomes `'A'`;
/// an empty token becomes `"A"`.
///
/// The first character carries the token's version byte, so the result is still URL-safe base64
/// but never a token the server minted.
#[must_use]
pub fn tampered(token: &str) -> String {
    let mut chars = token.chars();
    match chars.next() {
        None => "A".to_owned(),
        Some(first) => {
            let replacement = if first == 'A' { 'B' } else { 'A' };
            std::iter::once(replacement).chain(chars).collect()
        }
    }
}

/// Passes when the answer is `200` and carries every dashboard security header with its value.
pub fn security_headers(answer: &Answer) -> Check {
    let name = names::api::UI_SECURITY_HEADERS;
    let mut failures: Vec<String> = status_is(answer, 200).into_iter().collect();
    for (header, value) in gang_ui::SECURITY_HEADERS {
        let got = answer.header(header);
        if got != Some(value) {
            failures.push(format!("{header} is {got:?}, not {value:?}"));
        }
    }
    verdict(name, &failures)
}

/// The first attribute value in `html`, in document order, introduced by `src="` or `href="`, that
/// starts with `/ui/` and ends with `.js` or `.css`.
#[must_use]
pub fn hashed_asset(html: &str) -> Option<String> {
    let mut values: Vec<(usize, &str)> = Vec::new();
    for prefix in ["src=\"", "href=\""] {
        for (at, _) in html.match_indices(prefix) {
            let start = at + prefix.len();
            let Some(rest) = html.get(start..) else {
                continue;
            };
            let value = rest.split('"').next().unwrap_or_default();
            values.push((at, value));
        }
    }
    values.sort_by_key(|(at, _)| *at);
    values
        .into_iter()
        .map(|(_, value)| value)
        .find(|value| {
            let extension = std::path::Path::new(value).extension();
            value.starts_with("/ui/")
                && extension.is_some_and(|extension| {
                    extension.eq_ignore_ascii_case("js") || extension.eq_ignore_ascii_case("css")
                })
        })
        .map(str::to_owned)
}

/// Passes when a hashed asset is answered `200` brotli-encoded with a non-empty body.
pub fn asset_brotli(answer: &Answer) -> Check {
    let name = names::api::UI_ASSET_BROTLI;
    let mut failures: Vec<String> = status_is(answer, 200).into_iter().collect();
    let encoding = answer.header("content-encoding");
    if encoding != Some("br") {
        failures.push(format!("content-encoding is {encoding:?}, not \"br\""));
    }
    if answer.body.is_empty() {
        failures.push("the body is empty".to_owned());
    }
    verdict(name, &failures)
}

/// Passes when a hashed asset is answered `200` as an immutable one-year cache entry.
pub fn asset_immutable(answer: &Answer) -> Check {
    let name = names::api::UI_ASSET_IMMUTABLE;
    let mut failures: Vec<String> = status_is(answer, 200).into_iter().collect();
    let cache = answer.header("cache-control");
    if cache != Some("public, max-age=31536000, immutable") {
        failures.push(format!(
            "cache-control is {cache:?}, not \"public, max-age=31536000, immutable\""
        ));
    }
    verdict(name, &failures)
}

/// Passes when a hashed asset is answered an empty `304` to its `ETag`.
pub fn asset_revalidates(answer: &Answer) -> Check {
    let name = names::api::UI_ASSET_REVALIDATES;
    let mut failures: Vec<String> = status_is(answer, 304).into_iter().collect();
    if !answer.body.is_empty() {
        failures.push(format!("the body is not empty: {}", answer.text()));
    }
    verdict(name, &failures)
}

/// Passes when a cross-origin logout is answered an empty `403` with no `set-cookie` header.
pub fn logout_refused(answer: &Answer) -> Check {
    let name = names::api::CROSS_ORIGIN_LOGOUT_REFUSED;
    let mut failures: Vec<String> = status_is(answer, 403).into_iter().collect();
    if !answer.body.is_empty() {
        failures.push(format!("the body is not empty: {}", answer.text()));
    }
    if answer
        .headers
        .iter()
        .any(|(header, _)| header == "set-cookie")
    {
        failures.push("a set-cookie header is present".to_owned());
    }
    verdict(name, &failures)
}

/// Passes when the cookie-only revoke answer is `200`: the named principal is still admitted.
pub fn still_admitted(answer: &Answer) -> Check {
    match status_is(answer, 200) {
        None => Check::passed(names::api::COOKIE_POST_REVOKES_NOTHING),
        Some(why) => Check::failed(names::api::COOKIE_POST_REVOKES_NOTHING, why),
    }
}

/// One block of a server-sent event stream.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SseBlock {
    /// The `event:` field, if the block has one.
    pub event: Option<String>,
    /// The `data:` lines joined by `'\n'`; empty when the block has none.
    pub data: String,
    /// The raw `retry:` value, exactly as sent.
    pub retry: Option<String>,
    /// The `id:` value, if the block has one.
    pub id: Option<String>,
}

/// Drains every complete block (terminated by a blank line) from the front of `buffer` and returns
/// them in order; bytes after the last blank line stay in `buffer`.
pub fn take_blocks(buffer: &mut Vec<u8>) -> Vec<SseBlock> {
    let mut blocks = Vec::new();
    while let Some(end) = buffer.windows(2).position(|pair| pair == b"\n\n") {
        let text = String::from_utf8_lossy(&buffer[..end]);
        blocks.push(parse_block(&text));
        buffer.drain(..end + 2);
    }
    blocks
}

/// Parses one block's text, without its terminating blank line, into its fields.
fn parse_block(text: &str) -> SseBlock {
    let mut block = SseBlock::default();
    let mut data = String::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => block.event = Some(value.to_owned()),
            "data" => {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(value);
            }
            "retry" => block.retry = Some(value.to_owned()),
            "id" => block.id = Some(value.to_owned()),
            _ => {}
        }
    }
    block.data = data;
    block
}

/// The `closed` event `block` is, if it is one and its data parses.
#[must_use]
pub fn closed_of(block: &SseBlock) -> Option<Closed> {
    if block.event.as_deref() != Some(CLOSED_EVENT) {
        return None;
    }
    serde_json::from_str::<Closed>(&block.data).ok()
}

/// How a read of a stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    /// The awaited event arrived.
    Stopped,
    /// The body ended first.
    Ended,
    /// The deadline passed first.
    TimedOut,
}

/// Passes when some block is a `closed` event for `SHUTDOWN`.
pub fn shutdown_closed(blocks: &[SseBlock], ending: Ending) -> Check {
    let name = names::api::SHUTDOWN_CLOSES_STREAM;
    let shutdown = blocks
        .iter()
        .filter_map(closed_of)
        .any(|closed| closed.reason == ClosedReason::Shutdown);
    if shutdown {
        return Check::passed(name);
    }
    let events: Vec<Option<&str>> = blocks.iter().map(|block| block.event.as_deref()).collect();
    Check::failed(
        name,
        format!("no closed event: SHUTDOWN arrived (stream {ending:?}); events: {events:?}"),
    )
}

/// Passes when the server process exited with status 0.
pub fn exited_zero(exit: Exit) -> Check {
    let name = names::api::SHUTDOWN_EXITS_ZERO;
    if exit == Exit::Code(0) {
        Check::passed(name)
    } else {
        Check::failed(name, format!("the server ended {exit:?}"))
    }
}

/// Passes when the first viewer's state stream is answered `200` and its first block is a snapshot.
pub fn first_viewer_admitted(status: u16, blocks: &[SseBlock], ending: Ending) -> Check {
    let name = names::viewer_cap::FIRST_VIEWER_ADMITTED;
    let first = blocks.first().and_then(|block| block.event.as_deref());
    if status == 200 && first == Some(SNAPSHOT_EVENT) {
        return Check::passed(name);
    }
    Check::failed(
        name,
        format!(
            "answered {status}, ended {ending:?}, and its first event is {first:?}, not {SNAPSHOT_EVENT:?}"
        ),
    )
}

/// Passes when a viewer over the cap is answered `200` and its first block is a `closed` event for
/// `VIEWER_CAP` with the configured retry delay.
pub fn viewer_capped(status: u16, blocks: &[SseBlock], ending: Ending) -> Check {
    let name = names::viewer_cap::VIEWER_CAP_CLOSED;
    let capped = blocks.first().and_then(closed_of).is_some_and(|closed| {
        closed.reason == ClosedReason::ViewerCap && closed.retry_delay == Some(VIEWER_RETRY_AFTER)
    });
    if status == 200 && capped {
        return Check::passed(name);
    }
    let first = match blocks.first() {
        None => "absent".to_owned(),
        Some(block) => format!("{:?} with data {:?}", block.event, block.data),
    };
    Check::failed(
        name,
        format!(
            "answered {status}, ended {ending:?}; the first event is {first}, not a closed event: VIEWER_CAP with retryDelay {VIEWER_RETRY_AFTER:?}"
        ),
    )
}

/// Passes when the first closed event's SSE `retry:` line is its `retryDelay` in milliseconds.
pub fn retry_line_matches(blocks: &[SseBlock]) -> Check {
    let name = names::viewer_cap::RETRY_LINE_MATCHES;
    let Some((block, closed)) = blocks
        .iter()
        .find_map(|block| closed_of(block).map(|closed| (block, closed)))
    else {
        return Check::failed(name, "no closed event arrived");
    };
    let Some(delay) = closed.retry_delay else {
        return Check::failed(name, "the closed event carries no retryDelay");
    };
    let expected = delay.as_millis().to_string();
    if block.retry.as_deref() == Some(expected.as_str()) {
        Check::passed(name)
    } else {
        Check::failed(
            name,
            format!(
                "the closed event's SSE retry: line is {:?}, not {expected:?}",
                block.retry
            ),
        )
    }
}

/// The `retry-after` header of `answer` as whole seconds.
///
/// # Errors
///
/// When the header is missing, empty, not all ASCII digits, does not parse as a `u64`, or is `0`.
pub fn retry_after_seconds(answer: &Answer) -> std::result::Result<u64, String> {
    let value = answer
        .header("retry-after")
        .ok_or_else(|| "no retry-after header".to_owned())?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("retry-after is {value:?}, not whole seconds"));
    }
    let seconds: u64 = value.parse().map_err(|error| {
        format!(
            "retry-after {value:?} does not parse: {}",
            gang_types::error_chain(&error)
        )
    })?;
    if seconds < 1 {
        return Err(format!("retry-after is {seconds}, not at least 1"));
    }
    Ok(seconds)
}

/// Passes when no request of the burst was answered `429`.
pub fn burst_admitted(answers: &[(String, Answer)]) -> Check {
    let name = names::request_limits::BURST_ADMITTED;
    if answers.is_empty() {
        return Check::failed(name, "no request of the burst was made");
    }
    let refused: Vec<String> = answers
        .iter()
        .enumerate()
        .filter(|(_, (_, answer))| answer.status == 429)
        .map(|(index, (line, _))| format!("{line} (#{index})"))
        .collect();
    if refused.is_empty() {
        Check::passed(name)
    } else {
        Check::failed(
            name,
            format!("the burst was refused: {}", refused.join(", ")),
        )
    }
}

/// Passes when the request after the burst is refused as `RATE_LIMITED` with a whole-second
/// `Retry-After`.
pub fn api_rate_limited(answer: &Answer) -> Check {
    let name = names::request_limits::API_RATE_LIMITED;
    let mut failures = Vec::new();
    if let Err(why) = refusal(answer, ErrorReason::RateLimited, &[]) {
        failures.push(why);
    }
    if let Err(why) = retry_after_seconds(answer) {
        failures.push(why);
    }
    verdict(name, &failures)
}

/// Passes when the sign-in request after the burst is answered an empty `429` with a whole-second
/// `Retry-After`.
pub fn ui_rate_limited(answer: &Answer) -> Check {
    let name = names::request_limits::UI_RATE_LIMITED;
    let mut failures: Vec<String> = status_is(answer, 429).into_iter().collect();
    if !answer.body.is_empty() {
        failures.push(format!("the body is not empty: {}", answer.text()));
    }
    if let Err(why) = retry_after_seconds(answer) {
        failures.push(why);
    }
    verdict(name, &failures)
}

/// Passes when the handshake verified against the CA pin; a refusal fails with its error chain.
pub fn leaf_pinned(name: &'static str, handshake: &Handshake) -> Check {
    match handshake {
        Handshake::Verified(_) => Check::passed(name),
        Handshake::Refused(why) => Check::failed(name, why.as_str()),
    }
}

/// Passes when the leaf is valid for exactly [`CERTIFICATE_TTL`] plus [`CERTIFICATE_BACKDATE`].
pub fn leaf_lifetime(leaf: &Leaf) -> Check {
    let name = names::certificate_rotation::LEAF_LIFETIME;
    let expected = CERTIFICATE_TTL + CERTIFICATE_BACKDATE;
    match leaf.not_after.duration_since(leaf.not_before) {
        Ok(lifetime) if lifetime == expected => Check::passed(name),
        Ok(lifetime) => Check::failed(
            name,
            format!("the leaf is valid for {lifetime:?}, not {expected:?}"),
        ),
        Err(error) => Check::failed(
            name,
            format!(
                "the leaf's notAfter precedes its notBefore by {:?}",
                error.duration()
            ),
        ),
    }
}

/// The instant after which a connection must be shown a reissued leaf: `notBefore` plus
/// [`REISSUE_AT_PERCENT`] of [`CERTIFICATE_TTL`], plus [`REISSUE_SLACK`].
///
/// The server reissues `reissue_at_percent` of the ttl after issuing, and with no backdate the
/// leaf's `notBefore` is the issuing instant truncated to whole seconds.
#[must_use]
pub fn reissue_deadline(leaf: &Leaf) -> SystemTime {
    leaf.not_before + CERTIFICATE_TTL * REISSUE_AT_PERCENT / 100 + REISSUE_SLACK
}

/// Passes when the handshake after the reissue verified with a leaf whose serial differs from
/// `first`'s.
pub fn leaf_reissued(first: &Leaf, second: &Handshake) -> Check {
    let name = names::certificate_rotation::LEAF_REISSUED;
    match second {
        Handshake::Verified(leaf) if leaf.serial != first.serial => Check::passed(name),
        Handshake::Verified(leaf) => Check::failed(
            name,
            format!(
                "the reissued leaf has serial {}, the same as the first",
                hex::encode(&leaf.serial)
            ),
        ),
        Handshake::Refused(why) => Check::failed(name, why.as_str()),
    }
}

/// Passes when the connection established before the reissue still answers a ping.
pub fn established_kept(event: ChannelEvent) -> Check {
    let name = names::certificate_rotation::ESTABLISHED_KEPT;
    match event {
        ChannelEvent::PingAcknowledged => Check::passed(name),
        ChannelEvent::Closed => Check::failed(name, "the connection was closed"),
    }
}

/// The failure of `answer` not having `expected` as its status, quoting its body; `None` when the
/// status matches.
fn status_is(answer: &Answer, expected: u16) -> Option<String> {
    (answer.status != expected).then(|| {
        format!(
            "answered {}, not {expected}: {}",
            answer.status,
            answer.text()
        )
    })
}

/// Every rule a non-2xx answer breaks as an error envelope, one message per broken rule.
fn envelope_violations(answer: &Answer) -> Vec<String> {
    let response = match serde_json::from_slice::<ErrorResponse>(&answer.body) {
        Ok(response) => response,
        Err(error) => {
            return vec![format!(
                "the body is not an error envelope ({}): {}",
                gang_types::error_chain(&error),
                answer.text()
            )];
        }
    };
    let error = &response.error;
    let mut failures = Vec::new();
    if error.code != answer.status {
        failures.push(format!(
            "the envelope's code is {}, not the HTTP status {}",
            error.code, answer.status
        ));
    }
    if error.status.http() != answer.status {
        failures.push(format!(
            "the status {:?} answers HTTP {}, not {}",
            error.status,
            error.status.http(),
            answer.status
        ));
    }
    if error.message.is_empty() {
        failures.push("the message is empty".to_owned());
    }
    let infos = error_infos(&error.details);
    match infos.as_slice() {
        [info] => {
            if info.domain != ERROR_DOMAIN {
                failures.push(format!(
                    "the ErrorInfo domain is {:?}, not {ERROR_DOMAIN:?}",
                    info.domain
                ));
            }
            if info.reason.is_empty() {
                failures.push("the ErrorInfo reason is empty".to_owned());
            }
        }
        _ => failures.push(format!("{} ErrorInfo details, not 1", infos.len())),
    }
    failures
}

/// The `ErrorInfo` details among `details`, in order.
fn error_infos(details: &[Detail]) -> Vec<&gang_wire::ErrorInfo> {
    details
        .iter()
        .filter_map(|detail| match detail {
            Detail::ErrorInfo(info) => Some(info),
            _ => None,
        })
        .collect()
}

/// The check `name` with `failures` joined by `"; "`; passed when there are none.
fn verdict(name: &'static str, failures: &[String]) -> Check {
    if failures.is_empty() {
        Check::passed(name)
    } else {
        Check::failed(name, failures.join("; "))
    }
}
