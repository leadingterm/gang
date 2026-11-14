//! The pure verdicts of the identity scenario's device login (ADR-37).

use gang_types::Url;
use gang_wire::{
    ApproveDeviceAuthorizationResponse, DEVICE_APPROVED_PATH, DEVICE_PATH, ErrorResponse,
};
use serde_json::Value;

use crate::access::StoredSession;
use crate::access_verdict::{Verdict, query_value, redirected_to, set_cookies};
use crate::chromium::Navigation;
use crate::issuer::{DASHBOARD_CLIENT_ID, RefreshOutcome, RefreshRecord};
use crate::process::Ran;
use crate::server::Answer;

/// What a headless `gang login` printed for its device login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevicePrompt {
    /// The verification URI, `<endpoint>/ui/device`.
    pub verification_uri: String,
    /// The user code, `XXXX-XXXX`.
    pub user_code: String,
}

/// Finds the device verification URI and user code in standard error.
#[must_use]
pub fn device_prompt(stderr: &str) -> Option<DevicePrompt> {
    let verification_uri = stderr.split_whitespace().find_map(|token| {
        let token = token.trim_end_matches(['.', ',']);
        Url::parse(token)
            .ok()
            .filter(|url| url.path() == DEVICE_PATH)
            .map(|_| token.to_owned())
    })?;
    let user_code = stderr.split_whitespace().find_map(|token| {
        let token = token.trim_end_matches(['.', ',', ')']);
        let (first, second) = token.split_once('-')?;
        let valid = |part: &str| {
            part.len() == 4
                && part
                    .bytes()
                    .all(|byte| b"BCDFGHJKLMNPQRSTVWXZ".contains(&byte))
        };
        (valid(first) && valid(second)).then(|| token.to_owned())
    })?;
    Some(DevicePrompt {
        verification_uri,
        user_code,
    })
}

/// Checks that no browser sign-in URL was printed.
///
/// # Errors
///
/// Returns a description containing the offending line when a browser URL was printed.
pub fn device_only(stderr: &str) -> Verdict {
    if let Some(line) = stderr.lines().find(|line| line.contains("/authorize")) {
        Err(format!("printed a browser sign-in URL: {line}"))
    } else {
        Ok(())
    }
}

/// Checks the public device authorization resource.
///
/// # Errors
///
/// Returns a description when the status or resource fields do not match.
pub fn device_resource(answer: &Answer, code: &str, client_address: &str) -> Verdict {
    let body = String::from_utf8_lossy(&answer.body);
    if answer.status != 200 {
        return Err(format!("status {}, body {body}", answer.status));
    }
    let value: Value = serde_json::from_slice(&answer.body)
        .map_err(|error| format!("invalid device resource {body}: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| format!("device resource is not an object: {value}"))?;
    if object.contains_key("secret") {
        return Err("the Get carries the secret".to_owned());
    }
    let expected_name = format!("deviceAuthorizations/{code}");
    if object.get("name").and_then(Value::as_str) != Some(expected_name.as_str())
        || object.get("clientAddress").and_then(Value::as_str) != Some(client_address)
        || object.get("state").and_then(Value::as_str) != Some("PENDING")
    {
        return Err(format!(
            "device resource {value}; expected name {expected_name}, clientAddress {client_address}, state PENDING"
        ));
    }
    Ok(())
}

/// Checks the browser's destination and the resulting device authorization state.
///
/// # Errors
///
/// Returns the refusal, unexpected destination, or observed resource state.
pub fn browser_approved(navigation: &Navigation, endpoint: &Url, resource: &Answer) -> Verdict {
    match navigation {
        Navigation::Refused(message) => {
            return Err(format!("the browser refused the approval: {message}"));
        }
        Navigation::Stalled => {
            return Err(
                "the browser neither navigated nor reported a refusal after the approve click"
                    .to_owned(),
            );
        }
        Navigation::Committed(url)
            if url.origin() != endpoint.origin() || url.path() != DEVICE_APPROVED_PATH =>
        {
            return Err(format!("the browser stopped at {url}"));
        }
        Navigation::Committed(_) => {}
    }
    let state = serde_json::from_slice::<Value>(&resource.body)
        .ok()
        .and_then(|value| {
            value
                .get("state")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    if resource.status == 200 && state.as_deref() == Some("APPROVED") {
        Ok(())
    } else {
        Err(format!(
            "device status {}, state {state:?}",
            resource.status
        ))
    }
}

/// Checks the provider URL and device approval callback.
///
/// # Errors
///
/// Returns a description when the answer, provider redirect or session cookie does not match.
pub fn device_approved(
    started: &Answer,
    provider_location: &str,
    done: &Answer,
    session_cookie: &str,
) -> Verdict {
    if started.status != 200 {
        return Err(format!("the approval answered {}", started.status));
    }
    serde_json::from_slice::<ApproveDeviceAuthorizationResponse>(&started.body)
        .map_err(|error| format!("the approval answered an invalid provider URL: {error}"))?;
    if query_value(provider_location, "code").is_none() {
        return Err(format!(
            "provider redirect has no code: {provider_location}"
        ));
    }
    redirected_to(done, DEVICE_APPROVED_PATH)?;
    if set_cookies(done)
        .iter()
        .any(|cookie| cookie.name == session_cookie && !cookie.value.is_empty())
    {
        return Err("the approval set a session cookie".to_owned());
    }
    Ok(())
}

/// Checks that a cross-site device approval was refused.
///
/// # Errors
///
/// Returns a description when the status is not 403.
pub fn cross_site_approval_refused(answer: &Answer) -> Verdict {
    if answer.status == 403 {
        Ok(())
    } else {
        Err(format!(
            "status {}, location {:?}",
            answer.status,
            answer.header("location")
        ))
    }
}

/// Checks the status and structured error reason of a refusal.
///
/// # Errors
///
/// Returns the observed status and body when the refusal does not match.
pub fn refused_with(answer: &Answer, status: u16, reason: &str) -> Verdict {
    let matches_reason = serde_json::from_slice::<ErrorResponse>(&answer.body)
        .ok()
        .and_then(|response| response.error.reason())
        .is_some_and(|observed| observed.as_str() == reason);
    if answer.status == status && matches_reason {
        Ok(())
    } else {
        Err(format!(
            "status {}, body {}; expected status {status}, reason {reason}",
            answer.status,
            String::from_utf8_lossy(&answer.body)
        ))
    }
}

/// Checks that the login exited 0 and reported the provider it logged in with.
///
/// # Errors
///
/// Returns the outcome when the exit code or provider does not match.
pub fn logged_in_with(outcome: &Ran, provider: &str) -> Verdict {
    let message = format!("logged in with {provider}");
    if outcome.code == Some(0)
        && (outcome.stdout.contains(&message) || outcome.stderr.contains(&message))
    {
        Ok(())
    } else {
        Err(format!("expected {message:?}; outcome: {outcome:?}"))
    }
}

/// Checks the files of a device session.
///
/// # Errors
///
/// Returns the observed files when they do not describe a device session.
pub fn device_session(files: &StoredSession) -> Verdict {
    if files.kind.as_deref() == Some("device")
        && !files.id_token
        && files.refresh_token.is_some()
        && files.token_written.is_some()
    {
        Ok(())
    } else {
        Err(format!("not a device session: {files:?}"))
    }
}

/// Checks that the rotated refresh token was stored before the token.
///
/// # Errors
///
/// Returns a description when the stored token or write order does not match.
pub fn rotation_stored(files: &StoredSession, issued: &str) -> Verdict {
    if files.refresh_token.as_deref() != Some(issued) {
        return Err(format!(
            "stored refresh token is not the rotated one: {:?}, expected {issued:?}",
            files.refresh_token
        ));
    }
    if !matches!((files.refresh_written, files.token_written), (Some(refresh), Some(token)) if refresh <= token)
    {
        return Err(format!(
            "the token was written before the rotated refresh token: refresh {:?}, token {:?}",
            files.refresh_written, files.token_written
        ));
    }
    Ok(())
}

/// Checks that no session files were stored.
///
/// # Errors
///
/// Returns the observed files when the home is not empty.
pub fn stored_nothing(files: &StoredSession) -> Verdict {
    if *files == StoredSession::default() {
        Ok(())
    } else {
        Err(format!("session files were stored: {files:?}"))
    }
}

/// Checks that one dashboard-client refresh grant rotated the token.
///
/// # Errors
///
/// Returns the observed grants when they do not match.
pub fn renewed_by_web_client(grants: &[RefreshRecord]) -> std::result::Result<String, String> {
    if let [
        RefreshRecord {
            client_id,
            outcome: RefreshOutcome::Rotated { issued },
            ..
        },
    ] = grants
        && client_id == DASHBOARD_CLIENT_ID
    {
        Ok(issued.clone())
    } else {
        Err(format!(
            "expected one rotating dashboard refresh grant: {grants:?}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_requires_provider_url_and_valid_callback() {
        let started = Answer {
            status: 200,
            headers: vec![],
            body: br#"{"authorizationUrl":"https://example.com/authorize"}"#.to_vec(),
        };
        let done = Answer {
            status: 302,
            headers: vec![("location".into(), DEVICE_APPROVED_PATH.into())],
            body: vec![],
        };
        let provider = "https://example.com/ui/callback?code=ok";
        assert!(device_approved(&started, provider, &done, "session").is_ok());
        assert!(
            device_approved(
                &Answer {
                    status: 303,
                    ..started.clone()
                },
                provider,
                &done,
                "session"
            )
            .is_err()
        );
        assert!(
            device_approved(
                &Answer {
                    body: b"{}".to_vec(),
                    ..started.clone()
                },
                provider,
                &done,
                "session"
            )
            .is_err()
        );
        assert!(
            device_approved(
                &started,
                "https://example.com/ui/callback",
                &done,
                "session"
            )
            .is_err()
        );
        assert!(
            device_approved(
                &started,
                provider,
                &Answer {
                    status: 200,
                    ..done.clone()
                },
                "session"
            )
            .is_err()
        );
        let with_cookie = Answer {
            headers: vec![
                ("location".into(), DEVICE_APPROVED_PATH.into()),
                ("set-cookie".into(), "session=value; Path=/".into()),
            ],
            ..done
        };
        assert!(device_approved(&started, provider, &with_cookie, "session").is_err());
    }

    #[test]
    fn browser_approval_checks_navigation_and_state() {
        let endpoint = Url::parse("https://example.com/").expect("valid endpoint");
        let good = Navigation::Committed(endpoint.join(DEVICE_APPROVED_PATH).expect("valid path"));
        let answer = |status, body: &str| Answer {
            status,
            headers: vec![],
            body: body.as_bytes().to_vec(),
        };
        assert!(
            browser_approved(&good, &endpoint, &answer(200, r#"{"state":"APPROVED"}"#)).is_ok()
        );
        assert_eq!(
            browser_approved(
                &Navigation::Refused("blocked".into()),
                &endpoint,
                &answer(200, "{}")
            ),
            Err("the browser refused the approval: blocked".into())
        );
        assert_eq!(
            browser_approved(&Navigation::Stalled, &endpoint, &answer(200, "{}")),
            Err(
                "the browser neither navigated nor reported a refusal after the approve click"
                    .into()
            )
        );
        for url in [
            "https://example.com/other",
            "https://other.example/ui/device/approved",
        ] {
            let navigation = Navigation::Committed(Url::parse(url).expect("valid URL"));
            assert_eq!(
                browser_approved(&navigation, &endpoint, &answer(200, "{}")),
                Err(format!("the browser stopped at {url}"))
            );
        }
        assert!(
            browser_approved(&good, &endpoint, &answer(200, r#"{"state":"PENDING"}"#))
                .expect_err("pending fails")
                .contains("PENDING")
        );
        assert!(
            browser_approved(&good, &endpoint, &answer(500, r#"{"state":"APPROVED"}"#)).is_err()
        );
        assert!(browser_approved(&good, &endpoint, &answer(200, "invalid")).is_err());
    }
}
