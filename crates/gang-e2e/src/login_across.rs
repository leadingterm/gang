//! Device and browser login transactions continued by another controller.

use gang_tls::{HttpProtocol, HttpRequest, HttpTransport, RequestBody, ServerTrust};
use gang_types::Url;
use gang_wire::{DEVICE_PATH, LOGIN_PATH};
use http::Method;
use http::header::{AUTHORIZATION, COOKIE, HeaderName, HeaderValue};

use crate::access_verdict::{login_cookie_name, redirected_to, set_cookies};
use crate::check::Check;
use crate::cluster::Cluster;
use crate::device_verdict::{device_approved, device_resource, refused_with};
use crate::errors::{E2eError, Result};
use crate::http::Http;
use crate::names::login_across::{BROWSER, DEVICE, REPLAY};
use crate::server::{Answer, Presented, Request};
use crate::session::{SessionCookie, cookie_name};

const DEVICES: &str = "/v1/deviceAuthorizations";
const SESSION: &str = "/v1/session";

/// Sends one request to a specified direct listener, preserving cookies and bearer credentials.
pub(crate) fn exchange(client: &Http, endpoint: &Url, request: &Request) -> Result<Answer> {
    let url = endpoint
        .join(&request.target)
        .map_err(E2eError::parse("the request url"))?;
    let mut headers = Vec::new();
    let credential = match &request.presented {
        Presented::Nothing => None,
        Presented::Bearer(token) => Some((AUTHORIZATION, format!("Bearer {token}"))),
        Presented::Cookie(cookie) => Some((COOKIE, cookie.header())),
    };
    if let Some((name, value)) = credential {
        let mut value = HeaderValue::from_str(&value)
            .map_err(E2eError::system_from("building the credential header"))?;
        value.set_sensitive(true);
        headers.push((name, value));
    }
    for (name, value) in &request.headers {
        headers.push((
            HeaderName::from_bytes(name.as_bytes())
                .map_err(E2eError::system_from("building a request header name"))?,
            HeaderValue::from_str(value)
                .map_err(E2eError::system_from("building a request header value"))?,
        ));
    }
    let body = request.body.as_ref().map(|body| RequestBody {
        content_type: body.content_type,
        bytes: body.bytes.clone().into(),
    });
    let reply = client.send(HttpRequest {
        method: request.method.clone(),
        url: &url,
        bearer: None,
        headers: &headers,
        body,
    })?;
    Ok(Answer {
        status: reply.status.as_u16(),
        headers: reply
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect(),
        body: reply.body.to_vec(),
    })
}

fn visit(client: &Http, url: &Url) -> Result<Answer> {
    let reply = client.send(HttpRequest {
        method: Method::GET,
        url,
        bearer: None,
        headers: &[],
        body: None,
    })?;
    Ok(Answer {
        status: reply.status.as_u16(),
        headers: reply
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect(),
        body: reply.body.to_vec(),
    })
}

/// Visits the issuer and sends its callback to the chosen controller with the login cookie.
fn callback(
    client: &Http,
    issuer: &Http,
    endpoint: &Url,
    public: &Url,
    started: &Answer,
    authorization: &Url,
) -> Result<(String, Answer)> {
    let provider = visit(issuer, authorization)?;
    let location = provider
        .header("location")
        .filter(|_| provider.status == 302)
        .ok_or_else(|| {
            E2eError::system(format!("provider did not redirect: {}", provider.status))
        })?;
    let back = Url::parse(location).map_err(E2eError::parse("the provider callback url"))?;
    let name = login_cookie_name(public);
    let cookie = set_cookies(started)
        .into_iter()
        .find(|cookie| cookie.name == name && !cookie.value.is_empty())
        .ok_or_else(|| E2eError::system("sign-in did not set a login cookie"))?;
    let done = exchange(
        client,
        endpoint,
        &Request::get(format!(
            "{}?{}",
            back.path(),
            back.query().unwrap_or_default()
        ))
        .presenting(Presented::Cookie(SessionCookie {
            name: cookie.name,
            value: cookie.value,
        })),
    )?;
    Ok((location.to_owned(), done))
}

fn redeem(client: &Http, endpoint: &Url, code: &str, secret: &str) -> Result<Answer> {
    exchange(
        client,
        endpoint,
        &Request::new(Method::POST, format!("{DEVICES}/{code}:redeem")).body(
            "application/json",
            serde_json::json!({"secret": secret})
                .to_string()
                .into_bytes(),
        ),
    )
}

fn judged(name: &'static str, verdict: std::result::Result<(), String>) -> Check {
    match verdict {
        Ok(()) => Check::passed(name),
        Err(reason) => Check::failed(name, reason),
    }
}

/// Creates and starts approving a device authorization before leadership changes.
fn device_checks(
    cluster: &Cluster<'_>,
    client: &Http,
    issuer: &Http,
    leader: usize,
    public: &Url,
) -> Result<(Check, Check, usize, Option<String>)> {
    let a = cluster.direct_endpoint(leader);
    let created = exchange(
        client,
        &a,
        &Request::new(Method::POST, DEVICES).body("application/json", b"{}".to_vec()),
    )?;
    let value: serde_json::Value = serde_json::from_slice(&created.body)
        .map_err(E2eError::system_from("parsing device authorization"))?;
    let code = value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .and_then(|name| name.strip_prefix("deviceAuthorizations/"))
        .filter(|_| created.status == 200)
        .ok_or_else(|| {
            E2eError::system(format!(
                "device creation returned status {} without a code",
                created.status
            ))
        })?;
    let secret = value
        .get("secret")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| E2eError::system("device creation returned no secret"))?;
    let pending = exchange(client, &a, &Request::get(format!("{DEVICES}/{code}")))?;
    let body = serde_json::to_vec(&gang_wire::ApproveDeviceAuthorizationRequest {
        user_code: code.to_owned(),
    })
    .map_err(E2eError::system_from("serializing device approval"))?;
    let started = exchange(
        client,
        &a,
        &Request::new(Method::POST, DEVICE_PATH)
            .header("origin", public.origin().ascii_serialization())
            .header("sec-fetch-site", "same-origin")
            .body("application/json", body),
    )?;
    let during_approval = exchange(client, &a, &Request::get(format!("{DEVICES}/{code}")))?;
    let approval: gang_wire::ApproveDeviceAuthorizationResponse =
        serde_json::from_slice(&started.body)
            .map_err(E2eError::system_from("parsing device approval"))?;
    cluster.retire(leader)?;
    let next = cluster.wait_for_leader_except(Some(leader))?;
    cluster.wait_for_single_leader(next)?;
    let b = cluster.direct_endpoint(next);
    let (provider_location, done) = callback(
        client,
        issuer,
        &b,
        public,
        &started,
        &approval.authorization_url,
    )?;
    let redeemed = redeem(client, &b, code, secret)?;
    let token = serde_json::from_slice::<serde_json::Value>(&redeemed.body)
        .ok()
        .and_then(|value| {
            value
                .get("token")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    let status = token
        .as_ref()
        .map(|token| {
            exchange(
                client,
                &b,
                &Request::get(SESSION).presenting(Presented::Bearer(token.clone())),
            )
        })
        .transpose()?
        .map(|answer| answer.status);
    let device = device_resource(&pending, code, "127.0.0.1")
        .and_then(|()| device_resource(&during_approval, code, "127.0.0.1"))
        .and_then(|()| device_approved(&started, &provider_location, &done, cookie_name(public)))
        .and_then(|()| {
            if redeemed.status == 200 && status == Some(200) {
                Ok(())
            } else {
                Err(format!(
                    "redeem status {}; bearer session status {status:?}",
                    redeemed.status
                ))
            }
        });
    let replay = redeem(client, &b, code, secret)?;
    let replay_verdict = if redeemed.status == 200 {
        refused_with(&replay, 404, "DEVICE_AUTHORIZATION_NOT_FOUND")
    } else {
        Err(format!(
            "first redeem failed with status {}",
            redeemed.status
        ))
    };
    Ok((
        judged(DEVICE, device),
        judged(REPLAY, replay_verdict),
        next,
        token,
    ))
}

/// A 200 session may be anonymous; both controllers must identify the same principal.
fn browser_sessions_match(a: &Answer, b: &Answer) -> std::result::Result<(), String> {
    let principal = |answer: &Answer| {
        (answer.status == 200)
            .then(|| serde_json::from_slice::<serde_json::Value>(&answer.body).ok())
            .flatten()
            .and_then(|value| {
                value
                    .get("principal")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .filter(|principal| !principal.is_empty())
    };
    let first = principal(a);
    let second = principal(b);
    if first.is_some() && first == second {
        Ok(())
    } else {
        Err(format!(
            "browser session principals differ or are anonymous: A={first:?}, B={second:?}"
        ))
    }
}

/// Preserves a browser session across a second leadership change.
fn browser_check(
    cluster: &Cluster<'_>,
    client: &Http,
    issuer: &Http,
    leader: usize,
    public: &Url,
    token: Option<&str>,
) -> Result<(Check, Check)> {
    let a = cluster.direct_endpoint(leader);
    let browser_start = exchange(
        client,
        &a,
        &Request::get(format!("{LOGIN_PATH}?next=%2Fui")),
    )?;
    let location = browser_start
        .header("location")
        .filter(|_| browser_start.status == 302)
        .ok_or_else(|| {
            E2eError::system(format!(
                "browser login did not redirect: {}",
                browser_start.status
            ))
        })?;
    let authorization = Url::parse(location).map_err(E2eError::parse("the authorization url"))?;
    let (_, browser_done) = callback(client, issuer, &a, public, &browser_start, &authorization)?;
    let browser = redirected_to(&browser_done, "/ui").and_then(|()| {
        if set_cookies(&browser_done)
            .iter()
            .any(|cookie| cookie.name == cookie_name(public) && !cookie.value.is_empty())
        {
            Ok(())
        } else {
            Err("browser callback did not set a session cookie".to_owned())
        }
    });
    let mut bearer_survived = false;
    let browser = browser.and_then(|()| {
        let cookie = set_cookies(&browser_done)
            .into_iter()
            .find(|cookie| cookie.name == cookie_name(public) && !cookie.value.is_empty())
            .ok_or_else(|| "browser callback did not set a session cookie".to_owned())?;
        let request = Request::get(SESSION).presenting(Presented::Cookie(SessionCookie {
            name: cookie.name,
            value: cookie.value,
        }));
        let before = exchange(client, &a, &request).map_err(|error| error.to_string())?;
        let bearer_before = token
            .map(|token| {
                exchange(
                    client,
                    &a,
                    &Request::get(SESSION).presenting(Presented::Bearer(token.to_owned())),
                )
            })
            .transpose()
            .map_err(|error| error.to_string())?;
        cluster
            .start_controller()
            .map_err(|error| error.to_string())?;
        cluster.retire(leader).map_err(|error| error.to_string())?;
        let next = cluster
            .wait_for_leader_except(Some(leader))
            .map_err(|error| error.to_string())?;
        cluster
            .wait_for_single_leader(next)
            .map_err(|error| error.to_string())?;
        let after = exchange(client, &cluster.direct_endpoint(next), &request)
            .map_err(|error| error.to_string())?;
        let bearer_after = token
            .map(|token| {
                exchange(
                    client,
                    &cluster.direct_endpoint(next),
                    &Request::get(SESSION).presenting(Presented::Bearer(token.to_owned())),
                )
            })
            .transpose()
            .map_err(|error| error.to_string())?;
        bearer_survived = bearer_before
            .as_ref()
            .is_some_and(|answer| answer.status == 200)
            && bearer_after
                .as_ref()
                .is_some_and(|answer| answer.status == 200);
        browser_sessions_match(&before, &after)
    });
    Ok((
        judged(BROWSER, browser),
        Check::expect(
            "login across: bearer token survives leader handoff",
            bearer_survived,
            || "bearer session did not succeed before and after handoff".to_owned(),
        ),
    ))
}

/// Exercises device and browser login across successive leader handoffs.
///
/// # Errors
/// Returns an error if an HTTP exchange fails or a prerequisite response cannot be parsed.
pub fn login_across(cluster: &Cluster<'_>) -> Result<Vec<Check>> {
    let stack = cluster.stack();
    let timeout = stack.settings().command_timeout;
    let pin = stack.ca_pin()?;
    let client = Http::try_new(
        HttpTransport::Tls {
            trust: ServerTrust::Pin(&pin),
        },
        HttpProtocol::Http2,
        timeout,
    )?;
    let issuer = Http::try_new(HttpTransport::Plain, HttpProtocol::Negotiated, timeout)?;
    let leader = cluster.wait_for_leader()?;
    let public = stack.endpoint()?;
    let (device, replay, next, token) = device_checks(cluster, &client, &issuer, leader, &public)?;
    let (browser, bearer) =
        browser_check(cluster, &client, &issuer, next, &public, token.as_deref())?;
    Ok(vec![device, browser, bearer, replay])
}

#[cfg(test)]
mod tests {
    use super::browser_sessions_match;
    use crate::server::Answer;

    #[test]
    fn anonymous_session_does_not_prove_a_cookie_crossed_controllers() -> gang_testkit::TestResult {
        let signed_in = Answer::json(
            200,
            &serde_json::json!({"principal": "principals/e2e%3Abrowser"}),
        )?;
        let anonymous = Answer::json(200, &serde_json::json!({"name": "session"}))?;
        assert!(browser_sessions_match(&signed_in, &anonymous).is_err());
        assert!(browser_sessions_match(&signed_in, &signed_in).is_ok());
        Ok(())
    }
}
