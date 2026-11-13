//! The pure verdicts of the sessions scenario over HTTP answers and the issuer's records.

use gang_wire::routes::ERROR_PATH;

use crate::access_verdict::{
    CookieValue, ExpectedCookie, MaxAge, Verdict, cookie_set, query_value, set_cookies,
};
use crate::issuer::{
    AuthorizationRecord, CONSENT_PROMPT, DASHBOARD_CLIENT_ID, OFFLINE_ACCESS, RefreshOutcome,
    RefreshRecord,
};
use crate::server::Answer;

/// The `Max-Age` of a set renewal cookie: 400 days, the browsers' cap.
pub const RENEWAL_COOKIE_MAX_AGE: u64 = 34_560_000;

/// Checks that the provider authorization requests offline access and consent.
///
/// # Errors
///
/// Returns a description when the location lacks either parameter.
pub fn offline_requested(location: &str) -> Verdict {
    if query_value(location, OFFLINE_ACCESS.0).as_deref() != Some(OFFLINE_ACCESS.1)
        || query_value(location, CONSENT_PROMPT.0).as_deref() != Some(CONSENT_PROMPT.1)
    {
        return Err(format!(
            "offline access and consent were not requested: {location}"
        ));
    }
    Ok(())
}

/// Checks that the provider authorization does not ask for offline access or consent.
///
/// # Errors
///
/// Returns a description when the location asks for offline access or consent.
pub fn consent_free(location: &str) -> Verdict {
    if query_value(location, OFFLINE_ACCESS.0).as_deref() == Some(OFFLINE_ACCESS.1) {
        return Err(format!("asks for offline access: {location}"));
    }
    if query_value(location, CONSENT_PROMPT.0).as_deref() == Some(CONSENT_PROMPT.1) {
        return Err(format!("asks for consent again: {location}"));
    }
    Ok(())
}

/// Checks the single authorization recorded for the dashboard client.
///
/// # Errors
///
/// Returns a description when the records do not match the expected authorization.
pub fn authorized(records: &[AuthorizationRecord], offline: bool) -> Verdict {
    if !matches!(records, [record] if record.client_id == DASHBOARD_CLIENT_ID
        && record.offline == offline && record.consent == offline)
    {
        return Err(format!("unexpected authorization records: {records:?}"));
    }
    Ok(())
}

/// Checks the attributes of a newly set renewal cookie.
///
/// # Errors
///
/// Returns a description when the renewal cookie does not match.
pub fn renewal_cookie(answer: &Answer, name: &str) -> Verdict {
    cookie_set(
        answer,
        &ExpectedCookie {
            name,
            value: CookieValue::NonEmpty,
            same_site: "Strict",
            max_age: MaxAge::Exactly(RENEWAL_COOKIE_MAX_AGE),
        },
    )
}

/// Checks that renewal succeeded and set a session cookie.
///
/// # Errors
///
/// Returns a description when the status or session cookie does not match.
pub fn renewed(answer: &Answer, session: &str) -> Verdict {
    if answer.status != 204 {
        return Err(format!("status {}", answer.status));
    }
    cookie_set(
        answer,
        &ExpectedCookie {
            name: session,
            value: CookieValue::NonEmpty,
            same_site: "Strict",
            max_age: MaxAge::Positive,
        },
    )
}

/// Checks that cross-site renewal was refused without touching cookies.
///
/// # Errors
///
/// Returns a description when the status or cookies do not match.
pub fn cross_site_renewal_refused(answer: &Answer) -> Verdict {
    let cookies = set_cookies(answer);
    if answer.status != 403 || !cookies.is_empty() {
        return Err(format!("status {}, cookies: {cookies:?}", answer.status));
    }
    Ok(())
}

/// Checks that a response has the expected status and leaves the renewal cookie alone.
///
/// # Errors
///
/// Returns a description when the status differs or the renewal cookie is touched.
pub fn renewal_kept(answer: &Answer, status: u16, name: &str) -> Verdict {
    let cookies = set_cookies(answer);
    if answer.status != status || cookies.iter().any(|cookie| cookie.name == name) {
        return Err(format!("status {}, cookies: {cookies:?}", answer.status));
    }
    Ok(())
}

/// Checks that a response has the expected status and clears the renewal cookie.
///
/// # Errors
///
/// Returns a description when the status or cleared cookie does not match.
pub fn renewal_cleared(answer: &Answer, status: u16, name: &str) -> Verdict {
    if answer.status != status {
        return Err(format!("status {}", answer.status));
    }
    cookie_set(
        answer,
        &ExpectedCookie {
            name,
            value: CookieValue::Exactly(""),
            same_site: "Strict",
            max_age: MaxAge::Exactly(0),
        },
    )
}

/// Checks that a sealed renewal cookie is not accepted as a credential.
///
/// # Errors
///
/// Returns a description when any credential presentation is accepted.
pub fn sealed_refused(bearer: &Answer, as_session: &Answer, authenticate: &Answer) -> Verdict {
    if bearer.status != 401 {
        return Err(format!(
            "accepted as a bearer token (status {})",
            bearer.status
        ));
    }
    if as_session.status != 401 {
        return Err(format!(
            "accepted as a session cookie (status {})",
            as_session.status
        ));
    }
    if (200..300).contains(&authenticate.status) {
        return Err(format!(
            "accepted by session:authenticate (status {})",
            authenticate.status
        ));
    }
    Ok(())
}

/// Checks that concurrent renewals shared one rotating refresh grant.
///
/// # Errors
///
/// Returns a description when either response or the provider grant does not match.
pub fn shared_grant(
    first: &Answer,
    second: &Answer,
    grants: &[RefreshRecord],
    session: &str,
    renewal: &str,
) -> Verdict {
    renewed(first, session).map_err(|error| format!("first renewal: {error}"))?;
    renewed(second, session).map_err(|error| format!("second renewal: {error}"))?;
    renewal_cookie(first, renewal).map_err(|error| format!("first renewal: {error}"))?;
    renewal_cookie(second, renewal).map_err(|error| format!("second renewal: {error}"))?;
    let [grant] = grants else {
        return Err(format!(
            "the provider saw {} refresh grants: {grants:?}",
            grants.len()
        ));
    };
    if grant.client_id != DASHBOARD_CLIENT_ID
        || !matches!(grant.outcome, RefreshOutcome::Rotated { .. })
    {
        return Err(format!("unexpected refresh grant: {grant:?}"));
    }
    Ok(())
}

/// Checks that renewals used only successful dashboard refresh grants.
///
/// # Errors
///
/// Returns a description when authorization or refresh records do not match.
pub fn silent_renewals(
    authorizations: &[AuthorizationRecord],
    grants: &[RefreshRecord],
    expected: usize,
) -> Verdict {
    if !authorizations.is_empty() {
        return Err(format!(
            "the provider saw authorization requests: {authorizations:?}"
        ));
    }
    if grants.len() != expected {
        return Err(format!(
            "the provider saw {} refresh grants, expected {expected}: {grants:?}",
            grants.len()
        ));
    }
    if let Some(grant) = grants.iter().find(|grant| {
        grant.client_id != DASHBOARD_CLIENT_ID
            || matches!(
                grant.outcome,
                RefreshOutcome::Refused | RefreshOutcome::ReuseDetected
            )
    }) {
        return Err(format!("unexpected refresh grant: {grant:?}"));
    }
    Ok(())
}

/// Checks that sign-in rechecked the held rotated refresh token once.
///
/// # Errors
///
/// Returns a description when the previous or re-check grant does not match.
pub fn rechecked(previous: Option<&RefreshRecord>, grants: &[RefreshRecord]) -> Verdict {
    let [grant] = grants else {
        return Err(format!(
            "the provider saw {} re-check grants: {grants:?}",
            grants.len()
        ));
    };
    if grant.client_id != DASHBOARD_CLIENT_ID
        || !matches!(
            grant.outcome,
            RefreshOutcome::Granted | RefreshOutcome::Rotated { .. }
        )
    {
        return Err(format!("unexpected re-check grant: {grant:?}"));
    }
    let Some(RefreshRecord {
        outcome: RefreshOutcome::Rotated { issued },
        ..
    }) = previous
    else {
        return Err(format!("previous grant was not a rotation: {previous:?}"));
    };
    if grant.refresh_token != *issued {
        return Err(format!(
            "the re-check presented {} instead of the held token {issued}",
            grant.refresh_token
        ));
    }
    Ok(())
}

/// Checks that the provider denied sign-in and the callback redirected to the error page.
///
/// # Errors
///
/// Returns a description when the provider location or callback response does not match.
pub fn refused_by_provider(provider_location: &str, done: &Answer) -> Verdict {
    if query_value(provider_location, "error").as_deref() != Some("access_denied")
        || query_value(provider_location, "code").is_some()
    {
        return Err(format!("unexpected provider location: {provider_location}"));
    }
    let location = done.header("location");
    if done.status != 302 || !location.is_some_and(|value| value.starts_with(ERROR_PATH)) {
        return Err(format!(
            "callback status {}, location: {location:?}",
            done.status
        ));
    }
    Ok(())
}
