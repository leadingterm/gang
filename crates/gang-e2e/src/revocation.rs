//! The revocation scenario: a token is accepted while valid, and once revoked its established
//! connection is denied on the next request, new connections and builds are refused, the server
//! is not restarted, and a restored token is accepted again.

use crate::check::Check;
use crate::errors::Result;
use crate::identity::{
    Admission, Channel, ChannelEvent, Credentials, Identity, Issuance, Presenting, Reply,
};
use crate::issuer::TokenSpec;
use crate::names;
use crate::verdict;

/// Issues a token, checks which connections, requests and builds are accepted, revokes it, and
/// judges whether the revocation took effect without a restart and whether restoring it works.
///
/// The connection that the revocation affects is opened right before the revocation, and proven
/// open by a ping it acknowledges.
///
/// # Errors
///
/// When a program cannot be run or its output cannot be understood.
pub fn revocation<I: Identity>(identity: &I) -> Result<Vec<Check>> {
    let credentials = match identity.issue(&TokenSpec::admitted("developer"))? {
        Issuance::Issued(credentials) => credentials,
        Issuance::Refused(why) => {
            return Ok(vec![Check::failed(names::revocation::TOKEN_ISSUED, why)]);
        }
    };
    let mut checks = Vec::new();

    checks.push(anonymous_request_is_unauthenticated(
        identity,
        &credentials,
    )?);
    let anonymous_build = identity.build_as(&credentials, Presenting::Nothing)?;
    checks.push(verdict::authentication_rejection(
        names::revocation::ANONYMOUS_BUILD,
        &anonymous_build,
    ));

    let mut channel = match identity.connect(&credentials, Presenting::Token)? {
        Admission::Accepted(channel) => channel,
        Admission::Refused => {
            checks.push(Check::failed(
                names::revocation::CONNECTION_ACCEPTED,
                "the server refused the TLS connection of a client with a token",
            ));
            return Ok(checks);
        }
    };
    checks.push(proves_open(&mut channel)?);

    let served = channel.request()?;
    checks.push(Check::expect(
        names::revocation::SERVED_BEFORE,
        matches!(served, Reply::Served),
        || format!("the request before revocation got {served:?}"),
    ));

    let before = identity.server_pid();
    identity.revoke(&credentials)?;

    let denied = channel.request()?;
    checks.push(Check::expect(
        names::revocation::OPEN_CONNECTION_DENIED,
        matches!(denied, Reply::Denied),
        || format!("the next request after revocation got {denied:?}"),
    ));

    let reconnect = reply_on_new_connection(identity, &credentials)?;
    checks.push(Check::expect(
        names::revocation::NEW_CONNECTION_DENIED,
        matches!(reconnect, Reply::Denied),
        || format!("a new connection with the revoked token got {reconnect:?}"),
    ));

    let revoked_build = identity.build_as(&credentials, Presenting::Token)?;
    checks.push(verdict::authentication_rejection(
        names::revocation::REVOKED_BUILD,
        &revoked_build,
    ));

    let after = identity.server_pid();
    checks.push(Check::expect(
        names::revocation::NO_RESTART,
        before.is_some() && before == after,
        || format!("the server process went from {before:?} to {after:?}"),
    ));

    identity.restore(&credentials)?;
    let restored = reply_on_new_connection(identity, &credentials)?;
    checks.push(Check::expect(
        names::revocation::RESTORED_CONNECTION,
        matches!(restored, Reply::Served),
        || format!("a new connection with the restored token got {restored:?}"),
    ));
    let restored_build = identity.build_as(&credentials, Presenting::Token)?;
    checks.push(Check::expect(
        names::revocation::RESTORED_BUILD,
        restored_build.succeeded(),
        || restored_build.exit_summary(),
    ));
    Ok(checks)
}

/// Opens a connection with the token and returns the reply to one request on it. A connection the
/// TLS layer refuses has no reply, so it is `Closed`.
///
/// # Errors
///
/// When the connection cannot be attempted at all.
fn reply_on_new_connection<I: Identity>(identity: &I, credentials: &Credentials) -> Result<Reply> {
    Ok(match identity.connect(credentials, Presenting::Token)? {
        Admission::Accepted(mut channel) => channel.request()?,
        Admission::Refused => Reply::Closed,
    })
}

/// Connects without a token and checks that the server answers a request as unauthenticated.
/// A TLS refusal is a failed check, since the public listener admits TLS without a token.
///
/// # Errors
///
/// When the connection cannot be attempted at all.
fn anonymous_request_is_unauthenticated<I: Identity>(
    identity: &I,
    credentials: &Credentials,
) -> Result<Check> {
    const NAME: &str = names::revocation::ANONYMOUS_REQUEST;
    Ok(match identity.connect(credentials, Presenting::Nothing)? {
        Admission::Accepted(mut channel) => {
            let reply = channel.request()?;
            Check::expect(NAME, matches!(reply, Reply::Unauthenticated), || {
                format!("a request without a token got {reply:?}")
            })
        }
        Admission::Refused => Check::failed(
            NAME,
            "the server refused the TLS connection of a client without a token",
        ),
    })
}

/// Pings `channel` and reports whether the server acknowledges the ping, which shows the
/// connection is open. A server that closes the connection instead is a failed check.
///
/// # Errors
///
/// Returns the errors of the channel's operations.
fn proves_open<C: Channel>(channel: &mut C) -> Result<Check> {
    const NAME: &str = names::revocation::CONNECTION_OPEN;
    Ok(match channel.ping()? {
        ChannelEvent::PingAcknowledged => Check::passed(NAME),
        ChannelEvent::Closed => Check::failed(
            NAME,
            "the server closed the connection before the revocation",
        ),
    })
}
