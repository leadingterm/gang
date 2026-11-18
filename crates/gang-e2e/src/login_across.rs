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

/// Creates, approves and redeems a device authorization across the two listeners.
fn device_checks(
    client: &Http,
    issuer: &Http,
    a: &Url,
    b: &Url,
    public: &Url,
) -> Result<(Check, Check)> {
    let created = exchange(
        client,
        a,
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
    let pending = exchange(client, b, &Request::get(format!("{DEVICES}/{code}")))?;
    let body = serde_json::to_vec(&gang_wire::ApproveDeviceAuthorizationRequest {
        user_code: code.to_owned(),
    })
    .map_err(E2eError::system_from("serializing device approval"))?;
    let started = exchange(
        client,
        a,
        &Request::new(Method::POST, DEVICE_PATH)
            .header("origin", public.origin().ascii_serialization())
            .header("sec-fetch-site", "same-origin")
            .body("application/json", body),
    )?;
    let during_approval = exchange(client, b, &Request::get(format!("{DEVICES}/{code}")))?;
    let approval: gang_wire::ApproveDeviceAuthorizationResponse =
        serde_json::from_slice(&started.body)
            .map_err(E2eError::system_from("parsing device approval"))?;
    let (provider_location, done) = callback(
        client,
        issuer,
        a,
        public,
        &started,
        &approval.authorization_url,
    )?;
    let redeemed = redeem(client, b, code, secret)?;
    let token = serde_json::from_slice::<serde_json::Value>(&redeemed.body)
        .ok()
        .and_then(|value| {
            value
                .get("token")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    let mut statuses = Vec::new();
    if let Some(token) = &token {
        for endpoint in [a, b] {
            statuses.push(
                exchange(
                    client,
                    endpoint,
                    &Request::get(SESSION).presenting(Presented::Bearer(token.clone())),
                )?
                .status,
            );
        }
    }
    let device = device_resource(&pending, code, "127.0.0.1")
        .and_then(|()| device_resource(&during_approval, code, "127.0.0.1"))
        .and_then(|()| device_approved(&started, &provider_location, &done, cookie_name(public)))
        .and_then(|()| {
            if redeemed.status == 200 && statuses == [200, 200] {
                Ok(())
            } else {
                Err(format!(
                    "redeem status {}; bearer session statuses {statuses:?}",
                    redeemed.status
                ))
            }
        });
    let replay = redeem(client, a, code, secret)?;
    let replay_verdict = if redeemed.status == 200 {
        refused_with(&replay, 404, "DEVICE_AUTHORIZATION_NOT_FOUND")
    } else {
        Err(format!(
            "first redeem failed with status {}",
            redeemed.status
        ))
    };
    Ok((judged(DEVICE, device), judged(REPLAY, replay_verdict)))
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

/// Starts browser sign-in on A and completes its issuer callback on B.
fn browser_check(client: &Http, issuer: &Http, a: &Url, b: &Url, public: &Url) -> Result<Check> {
    let browser_start = exchange(client, a, &Request::get(format!("{LOGIN_PATH}?next=%2Fui")))?;
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
    let (_, browser_done) = callback(client, issuer, b, public, &browser_start, &authorization)?;
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
    let start_on_b = exchange(client, b, &Request::get(format!("{LOGIN_PATH}?next=%2Fui")))?;
    let location = start_on_b
        .header("location")
        .filter(|_| start_on_b.status == 302)
        .ok_or_else(|| E2eError::system("browser login on B did not redirect"))?;
    let authorization = Url::parse(location).map_err(E2eError::parse("the authorization url"))?;
    let (_, done_on_a) = callback(client, issuer, a, public, &start_on_b, &authorization)?;
    let session = set_cookies(&done_on_a)
        .into_iter()
        .find(|cookie| cookie.name == cookie_name(public) && !cookie.value.is_empty());
    let browser = browser.and_then(|()| {
        let cookie = session
            .ok_or_else(|| "browser callback on A did not set a session cookie".to_owned())?;
        let request = Request::get(SESSION).presenting(Presented::Cookie(SessionCookie {
            name: cookie.name,
            value: cookie.value,
        }));
        let on_a = exchange(client, a, &request).map_err(|error| error.to_string())?;
        let on_b = exchange(client, b, &request).map_err(|error| error.to_string())?;
        browser_sessions_match(&on_a, &on_b)
    });
    Ok(judged(BROWSER, browser))
}

/// Exercises device and browser login against two direct public listeners.
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
    let a = cluster.direct_endpoint(0);
    let b = cluster.direct_endpoint(1);
    let public = stack.endpoint()?;
    let (device, replay) = device_checks(&client, &issuer, &a, &b, &public)?;
    let browser = browser_check(&client, &issuer, &a, &b, &public)?;
    Ok(vec![device, browser, replay])
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
