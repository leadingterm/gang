//! The sessions scenario (ADR-39): a signed-in browser renews its session silently from its
//! renewal cookie until the absolute lifetime, then signs in again without consent; revocation,
//! logout and a suspended account end renewal. The scenario sees the deployment only through
//! [`Browser`]; [`crate::browser_secured`] drives a real [`crate::Secured`] deployment with it.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use gang_types::Url;
use gang_wire::routes::{AUTHENTICATE_SESSION, GET_STATE, LOGIN_PATH, LOGOUT_PATH, RENEW_PATH};
use gang_wire::{ClosedReason, CookieNames, Frame};
use http::Method;

use crate::access_verdict::{
    CookieValue, ExpectedCookie, MaxAge, Verdict, cookie_set, redirected_to, set_cookies, status_is,
};
use crate::browser_verdict::{
    authorized, consent_free, cross_site_renewal_refused, offline_requested, rechecked,
    refused_by_provider, renewal_cleared, renewal_cookie, renewal_kept, renewed, sealed_refused,
    shared_grant, silent_renewals,
};
use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::issuer::{AuthorizationRecord, RefreshRecord, SubjectPolicy};
use crate::names::sessions as names;
use crate::server::{Answer, Presented, Request};
use crate::session::SessionCookie;
use crate::sse::EventStream;
use crate::template::PROVIDER;

/// The subject browsers sign in as in the sessions deployment (its `login_hint`).
pub const BROWSER_SUBJECT: &str = "browser";
/// `[authentication.clients.token] token_ttl` of the sessions deployment.
pub const SESSION_TOKEN_TTL: Duration = Duration::from_mins(1);
/// `[dashboard] session_max_age` of the sessions deployment.
pub const SESSION_MAX_AGE: Duration = Duration::from_mins(3);

/// What the sessions scenario sees of a deployment whose dashboard signs browsers in and renews their
/// sessions. Every method blocks until its observation is complete; an error is a harness failure,
/// what the deployment answers (refusals included) is `Ok`.
pub trait Browser: Sync {
    /// The open response of the state stream.
    type Stream: EventStream;
    /// The public endpoint.
    ///
    /// # Errors
    /// When no server has announced it.
    fn endpoint(&self) -> Result<Url>;
    /// Sends `request` to the public endpoint; redirects are not followed.
    ///
    /// # Errors
    /// When the request cannot be sent.
    fn request(&self, request: &Request) -> Result<Answer>;
    /// Gets the provider's absolute authorization URL without credentials or following redirects.
    ///
    /// # Errors
    /// When the request cannot be sent.
    fn visit(&self, url: &Url) -> Result<Answer>;
    /// Opens the state stream presenting `cookie`.
    ///
    /// # Errors
    /// When the stream cannot be opened.
    fn open_state_stream(&self, cookie: &SessionCookie) -> Result<Self::Stream>;
    /// Revokes the principal of `subject` under the deployment's provider, as the administrator.
    ///
    /// # Errors
    /// When the revocation cannot be performed.
    fn revoke(&self, subject: &str) -> Result<()>;
    /// Restores the principal of `subject`, as the administrator.
    ///
    /// # Errors
    /// When the restoration cannot be performed.
    fn restore(&self, subject: &str) -> Result<()>;
    /// The provider's authorization requests for `subject`, oldest first.
    fn authorizations(&self, subject: &str) -> Vec<AuthorizationRecord>;
    /// The provider's refresh grants for `subject`, oldest first.
    fn refreshes(&self, subject: &str) -> Vec<RefreshRecord>;
    /// Sets how the provider treats `subject` from now on.
    fn set_policy(&self, subject: &str, policy: SubjectPolicy);
}

/// UI fallback destination.
const UI_HOME: &str = "/ui";
/// An origin that is not the deployment's origin.
const FOREIGN_ORIGIN: &str = "https://evil.example";

/// Converts a verdict into a named check.
fn judged(name: &'static str, verdict: Verdict) -> Check {
    match verdict {
        Ok(()) => Check::passed(name),
        Err(reason) => Check::failed(name, reason),
    }
}

/// The browser's current cookies.
#[derive(Debug, Default)]
struct Jar(BTreeMap<String, String>);

impl Jar {
    /// Applies response cookies in their original order.
    fn absorb(&mut self, answer: &Answer) {
        for cookie in set_cookies(answer) {
            let expired = cookie
                .attribute("Max-Age")
                .flatten()
                .and_then(|v| v.parse::<i64>().ok())
                == Some(0);
            if cookie.value.is_empty() || expired {
                self.0.remove(&cookie.name);
            } else {
                self.0.insert(cookie.name, cookie.value);
            }
        }
    }

    /// Finds a cookie by name.
    fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// Adds the current cookies to a request.
    fn attach(&self, request: Request) -> Request {
        if self.0.is_empty() {
            request
        } else {
            let cookies = self
                .0
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ");
            request.header("cookie", cookies)
        }
    }
}

/// Marks a request as same-origin.
fn same_origin(request: Request, origin: &str) -> Request {
    request
        .header("origin", origin)
        .header("sec-fetch-site", "same-origin")
}

/// Builds a renewal request with the current cookies.
fn renewal(jar: &Jar, origin: &str) -> Request {
    jar.attach(same_origin(Request::new(Method::POST, RENEW_PATH), origin))
}

/// Extracts the current session cookie, or an empty one.
fn session(jar: &Jar, names: &CookieNames) -> SessionCookie {
    SessionCookie {
        name: names.session.to_owned(),
        value: jar.get(names.session).unwrap_or_default().to_owned(),
    }
}

/// Observations of a completed sign-in round trip.
struct SignIn {
    /// Authorization request location.
    location: String,
    /// Provider's callback location.
    provider_location: String,
    /// Callback response.
    done: Answer,
}

/// Performs a sign-in, distinguishing bad responses from harness failures.
fn sign_in<B: Browser>(browser: &B, jar: &mut Jar) -> Result<std::result::Result<SignIn, String>> {
    let started = browser.request(&jar.attach(Request::get(format!("{LOGIN_PATH}?next=%2Fui"))))?;
    jar.absorb(&started);
    let Some(location) = started.header("location").filter(|_| started.status == 302) else {
        return Ok(Err(format!(
            "the sign-in answered {} without a redirect",
            started.status
        )));
    };
    let location = location.to_owned();
    let Ok(url) = Url::parse(&location) else {
        return Ok(Err(format!("the authorization URL is invalid: {location}")));
    };
    let provider = browser.visit(&url)?;
    let Some(provider_location) = provider.header("location") else {
        return Ok(Err(format!(
            "the provider answered {} without a redirect",
            provider.status
        )));
    };
    let provider_location = provider_location.to_owned();
    let Ok(back) = Url::parse(&provider_location) else {
        return Ok(Err(format!(
            "the authorization URL is invalid: {provider_location}"
        )));
    };
    let done = browser.request(&jar.attach(Request::get(format!(
        "{}?{}",
        back.path(),
        back.query().unwrap_or_default()
    ))))?;
    jar.absorb(&done);
    Ok(Ok(SignIn {
        location,
        provider_location,
        done,
    }))
}

/// Waits for a stream to close within its budget.
fn closing(
    stream: &mut impl EventStream,
    budget: Duration,
) -> Result<std::result::Result<ClosedReason, String>> {
    if stream.status() != 200 {
        return Ok(Err(format!(
            "the state stream answered {}",
            stream.status()
        )));
    }
    let deadline = Instant::now() + budget;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(Err(format!("no closed frame within {budget:?}")));
        }
        match stream.next_event_within(remaining) {
            Ok(Some(Frame::Closed(closed))) => return Ok(Ok(closed.reason)),
            Ok(Some(_)) => {}
            Ok(None) => {
                return Ok(Err(
                    "the state stream ended without a closed frame".to_owned()
                ));
            }
            Err(E2eError::Timeout { .. }) => {
                return Ok(Err(format!("no closed frame within {budget:?}")));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Checks the reason a stream closed.
fn expect_reason(
    reason: std::result::Result<ClosedReason, String>,
    expected: &ClosedReason,
) -> Verdict {
    match reason {
        Ok(actual) if &actual == expected => Ok(()),
        Ok(other) => Err(format!("the stream ended with {other:?}, not {expected:?}")),
        Err(reason) => Err(reason),
    }
}

/// Waits for expiry, renews the session, and opens its replacement stream.
fn renewal_cycle<B: Browser>(
    browser: &B,
    jar: &mut Jar,
    stream: &mut B::Stream,
    origin: &str,
    names: &CookieNames,
    budget: Duration,
) -> Result<Verdict> {
    if let Err(reason) = expect_reason(closing(stream, budget)?, &ClosedReason::Expired) {
        return Ok(Err(reason));
    }
    let answer = browser.request(&renewal(jar, origin))?;
    jar.absorb(&answer);
    if let Err(reason) = renewed(&answer, names.session) {
        return Ok(Err(reason));
    }
    *stream = browser.open_state_stream(&session(jar, names))?;
    if stream.status() != 200 {
        return Ok(Err(format!(
            "the renewed session's stream answered {}",
            stream.status()
        )));
    }
    match stream.next_event_within(budget) {
        Ok(Some(Frame::Snapshot(_))) => Ok(Ok(())),
        Ok(_) | Err(E2eError::Timeout { .. }) => Ok(Err(
            "the reopened stream did not start with a snapshot".to_owned(),
        )),
        Err(error) => Err(error),
    }
}

/// The mutable state shared by the scenario's stages.
struct Scenario<'a, B> {
    /// Deployment under observation.
    browser: &'a B,
    /// Cookies accumulated by the browser.
    jar: Jar,
    /// Endpoint's origin.
    origin: String,
    /// Cookie names for the endpoint's scheme.
    cookies: CookieNames,
    /// Maximum wait for a session expiry.
    budget: Duration,
    /// Maximum wait for revocation.
    margin: Duration,
    /// Checks in scenario order.
    checks: Vec<Check>,
}

impl<B: Browser> Scenario<'_, B> {
    /// Checks refusal of cross-origin, missing and misused sealed credentials.
    fn credential_checks(&mut self) -> Result<()> {
        let sealed = self
            .jar
            .get(self.cookies.renewal)
            .unwrap_or_default()
            .to_owned();
        let cross = self.browser.request(
            &self
                .jar
                .attach(Request::new(Method::POST, RENEW_PATH))
                .header("origin", FOREIGN_ORIGIN)
                .header("sec-fetch-site", "cross-site"),
        )?;
        self.checks.push(judged(
            names::CROSS_SITE_RENEWAL_REFUSED,
            cross_site_renewal_refused(&cross),
        ));
        let bare = self.browser.request(&same_origin(
            Request::new(Method::POST, RENEW_PATH),
            &self.origin,
        ))?;
        self.checks
            .push(judged(names::RENEWAL_NEEDS_COOKIE, status_is(&bare, 401)));
        let bearer = self.browser.request(
            &Request::get(GET_STATE.template).presenting(Presented::Bearer(sealed.clone())),
        )?;
        let as_session = self
            .browser
            .request(
                &Request::get(GET_STATE.template).presenting(Presented::Cookie(SessionCookie {
                    name: self.cookies.session.to_owned(),
                    value: sealed.clone(),
                })),
            )?;
        let body = serde_json::to_vec(&serde_json::json!({"provider": PROVIDER, "proof": sealed}))
            .map_err(|error| {
                E2eError::system(format!("cannot serialize session proof: {error}"))
            })?;
        let authenticate = self.browser.request(
            &Request::new(Method::POST, AUTHENTICATE_SESSION.template)
                .body("application/json", body),
        )?;
        self.checks.push(judged(
            names::SEALED_COOKIE_NOT_A_CREDENTIAL,
            sealed_refused(&bearer, &as_session, &authenticate),
        ));
        Ok(())
    }

    /// Checks concurrent renewals and follows them with another renewal.
    fn concurrent(&mut self) -> Result<()> {
        let before = self.browser.refreshes(BROWSER_SUBJECT).len();
        let request = renewal(&self.jar, &self.origin);
        let browser = self.browser;
        let (a, b) = std::thread::scope(|scope| {
            let first = scope.spawn(|| browser.request(&request));
            let second = scope.spawn(|| browser.request(&request));
            let a = first
                .join()
                .map_err(|_| E2eError::system("a concurrent renewal panicked"))?;
            let b = second
                .join()
                .map_err(|_| E2eError::system("a concurrent renewal panicked"))?;
            Ok::<_, E2eError>((a?, b?))
        })?;
        self.jar.absorb(&a);
        self.jar.absorb(&b);
        let grants = self.browser.refreshes(BROWSER_SUBJECT);
        let pair = grants.get(before..).unwrap_or_default().to_vec();
        let after = self.browser.request(&renewal(&self.jar, &self.origin))?;
        self.jar.absorb(&after);
        self.checks.push(judged(
            names::CONCURRENT_RENEWALS_SHARE_ONE_GRANT,
            shared_grant(&a, &b, &pair, self.cookies.session, self.cookies.renewal)
                .and_then(|()| renewed(&after, self.cookies.session)),
        ));
        Ok(())
    }

    /// Checks two expiry cycles and the absolute lifetime, then signs in again.
    fn expiry(&mut self) -> Result<()> {
        let authorizations_mark = self.browser.authorizations(BROWSER_SUBJECT).len();
        let refreshes_mark = self.browser.refreshes(BROWSER_SUBJECT).len();
        let mut stream = self
            .browser
            .open_state_stream(&session(&self.jar, &self.cookies))?;
        let mut cycles = Ok(());
        for _ in 0..2 {
            cycles = renewal_cycle(
                self.browser,
                &mut self.jar,
                &mut stream,
                &self.origin,
                &self.cookies,
                self.budget,
            )?;
            if cycles.is_err() {
                break;
            }
        }
        self.checks
            .push(judged(names::RENEWS_ACROSS_EXPIRY, cycles));
        let authorizations = self.browser.authorizations(BROWSER_SUBJECT);
        let refreshes = self.browser.refreshes(BROWSER_SUBJECT);
        self.checks.push(judged(
            names::RENEWAL_SILENT,
            silent_renewals(
                authorizations
                    .get(authorizations_mark..)
                    .unwrap_or_default(),
                refreshes.get(refreshes_mark..).unwrap_or_default(),
                2,
            ),
        ));
        let ended = expect_reason(closing(&mut stream, self.budget)?, &ClosedReason::Expired);
        let late = self.browser.request(&renewal(&self.jar, &self.origin))?;
        self.jar.absorb(&late);
        self.checks.push(judged(
            names::MAX_AGE_ENDS_RENEWAL,
            ended.and_then(|()| renewal_kept(&late, 401, self.cookies.renewal)),
        ));
        let authorizations_mark = self.browser.authorizations(BROWSER_SUBJECT).len();
        let grants = self.browser.refreshes(BROWSER_SUBJECT);
        let refreshes_mark = grants.len();
        let previous = refreshes_mark
            .checked_sub(1)
            .and_then(|i| grants.get(i))
            .cloned();
        let second = sign_in(self.browser, &mut self.jar)?;
        let authorizations = self.browser.authorizations(BROWSER_SUBJECT);
        let refreshes = self.browser.refreshes(BROWSER_SUBJECT);
        let verdict = second.and_then(|s| {
            consent_free(&s.location)
                .and_then(|()| {
                    authorized(
                        authorizations
                            .get(authorizations_mark..)
                            .unwrap_or_default(),
                        false,
                    )
                })
                .and_then(|()| {
                    rechecked(
                        previous.as_ref(),
                        refreshes.get(refreshes_mark..).unwrap_or_default(),
                    )
                })
                .and_then(|()| redirected_to(&s.done, UI_HOME))
                .and_then(|()| renewal_cookie(&s.done, self.cookies.renewal))
        });
        self.checks
            .push(judged(names::SIGN_IN_WITHOUT_CONSENT, verdict));
        Ok(())
    }

    /// Checks revocation, logout, and provider suspension.
    fn termination(&mut self) -> Result<()> {
        let mut stream = self
            .browser
            .open_state_stream(&session(&self.jar, &self.cookies))?;
        let opened = if stream.status() == 200 {
            Ok(())
        } else {
            Err(format!("the stream answered {}", stream.status()))
        };
        self.browser.revoke(BROWSER_SUBJECT)?;
        let reason = closing(&mut stream, self.margin)?;
        let refused = self.browser.request(&renewal(&self.jar, &self.origin))?;
        self.jar.absorb(&refused);
        self.browser.restore(BROWSER_SUBJECT)?;
        self.checks.push(judged(
            names::REVOCATION_ENDS_RENEWAL,
            opened
                .and_then(|()| expect_reason(reason, &ClosedReason::Revoked))
                .and_then(|()| renewal_cleared(&refused, 403, self.cookies.renewal)),
        ));
        let fresh = sign_in(self.browser, &mut self.jar)?;
        let logout = self.browser.request(&self.jar.attach(same_origin(
            Request::new(Method::POST, LOGOUT_PATH),
            &self.origin,
        )))?;
        self.jar.absorb(&logout);
        let after = self.browser.request(&renewal(&self.jar, &self.origin))?;
        self.jar.absorb(&after);
        let verdict = fresh
            .map(drop)
            .and_then(|()| status_is(&logout, 204))
            .and_then(|()| {
                cookie_set(
                    &logout,
                    &ExpectedCookie {
                        name: self.cookies.session,
                        value: CookieValue::Exactly(""),
                        same_site: "Strict",
                        max_age: MaxAge::Exactly(0),
                    },
                )
            })
            .and_then(|()| renewal_cleared(&logout, 204, self.cookies.renewal))
            .and_then(|()| status_is(&after, 401));
        self.checks.push(judged(names::LOGOUT_CLEARS_BOTH, verdict));
        let fresh = sign_in(self.browser, &mut self.jar)?;
        self.browser
            .set_policy(BROWSER_SUBJECT, SubjectPolicy::Suspended);
        let refused = self.browser.request(&renewal(&self.jar, &self.origin))?;
        self.jar.absorb(&refused);
        let failed = sign_in(self.browser, &mut self.jar)?;
        self.browser
            .set_policy(BROWSER_SUBJECT, SubjectPolicy::Rotating);
        let verdict = fresh
            .map(drop)
            .and_then(|()| renewal_cleared(&refused, 401, self.cookies.renewal))
            .and_then(|()| failed.and_then(|s| refused_by_provider(&s.provider_location, &s.done)));
        self.checks
            .push(judged(names::SUSPENSION_ENDS_RENEWAL, verdict));
        Ok(())
    }
}

/// Runs the sessions scenario through `browser`; `margin` bounds each wait beyond a token lifetime.
///
/// With zero clock skew, streams close at session expiry: two renewals occur before the three-minute
/// absolute deadline, and the third expiry falls after it if overhead is below one token lifetime.
///
/// # Errors
/// When a request cannot be sent or a stream cannot be read.
pub fn sessions<B: Browser>(browser: &B, margin: Duration) -> Result<Vec<Check>> {
    let endpoint = browser.endpoint()?;
    let mut scenario = Scenario {
        browser,
        jar: Jar::default(),
        origin: endpoint.origin().ascii_serialization(),
        cookies: CookieNames::for_https(endpoint.scheme() == "https"),
        budget: SESSION_TOKEN_TTL + margin,
        margin,
        checks: Vec::new(),
    };
    browser.set_policy(BROWSER_SUBJECT, SubjectPolicy::Rotating);
    let mark = browser.authorizations(BROWSER_SUBJECT).len();
    let first = match sign_in(browser, &mut scenario.jar)? {
        Ok(first) => first,
        Err(reason) => {
            return Ok(names::ALL
                .into_iter()
                .map(|name| {
                    Check::failed(
                        name,
                        format!("the first sign-in did not complete: {reason}"),
                    )
                })
                .collect());
        }
    };
    let authorizations = browser.authorizations(BROWSER_SUBJECT);
    scenario.checks.push(judged(
        names::OFFLINE_SIGN_IN,
        offline_requested(&first.location)
            .and_then(|()| authorized(authorizations.get(mark..).unwrap_or_default(), true)),
    ));
    scenario.checks.push(judged(
        names::RENEWAL_COOKIE_SET,
        redirected_to(&first.done, UI_HOME)
            .and_then(|()| renewal_cookie(&first.done, scenario.cookies.renewal)),
    ));
    scenario.credential_checks()?;
    scenario.concurrent()?;
    scenario.expiry()?;
    scenario.termination()?;
    Ok(scenario.checks)
}
