//! The browser sign-in routes under `/ui`: they start a login with an interactive identity
//! provider, complete it from the provider's redirect, and end a session.

use std::net::IpAddr;
use std::sync::Arc;

use gang_clock::Clock;
use gang_spi::{
    ApiRequestBody, ApiResponseBody, AuthenticationPort, CredentialLifecyclePort, LoginPort,
    LoginRequest, PortError, Rejection, TokenIssue, full_body,
};
use gang_types::ProviderName;
use gang_wire::{ERROR_PATH, UiRoute};
use http::HeaderMap;
use http::header::{ALLOW, HeaderValue, LOCATION, ORIGIN, RETRY_AFTER, SET_COOKIE};
use http::{Method, Request, Response, StatusCode};
use parking_lot::Mutex;
use tracing::{info, warn};

use crate::attempts::{Attempt, LoginAttempts};
use crate::client_address::ClientAddress;
use crate::cookies::cookie_value;
use crate::core::Core;
use crate::deployment::Stack;
use crate::rate_limit::RateLimiter;
use crate::responses::empty;
use crate::settings::LoginSettings;

/// The page a sign-in lands on when no `next` path is safe to return to.
const UI_HOME: &str = "/ui";

/// The browser routes of the session: logout and, when offered, the sign-in.
pub(crate) struct SessionRoutes<K: Stack>(Arc<Shared<K>>);

/// The state every clone of a [`SessionRoutes`] shares.
struct Shared<K: Stack> {
    core: Arc<Core<K>>,
    login: Option<MountedLogin<K::Clock, K::Identity>>,
}

/// The sign-in routes and the verifier of their interactive provider.
struct MountedLogin<C, V> {
    settings: LoginSettings,
    /// The verified interactive provider's verifier.
    verifier: Arc<V>,
    attempts: Mutex<LoginAttempts>,
    /// The shared limiter for sign-in and callback requests.
    limiter: Arc<RateLimiter<Arc<C>>>,
}

impl<K: Stack> Clone for SessionRoutes<K> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K: Stack> SessionRoutes<K> {
    /// Builds the routes. The sign-in routes are mounted only when `login` is offered, the client
    /// plugin issues sessions, and the login's provider is interactive.
    #[must_use]
    pub(crate) fn new(
        core: Arc<Core<K>>,
        login: Option<LoginSettings>,
        limiter: Arc<RateLimiter<Arc<K::Clock>>>,
    ) -> Self {
        let login = login.and_then(|settings| {
            if !core.clients.capabilities().sessions {
                return None;
            }
            let provider = core.host.providers.get(&settings.provider)?;
            provider.login()?;
            Some(MountedLogin {
                attempts: Mutex::new(LoginAttempts::new(settings.max_pending, settings.ttl.get())),
                verifier: Arc::clone(&provider.verifier),
                limiter,
                settings,
            })
        });
        if login.is_some() {
            core.cookies.warn_insecure();
        }
        Self(Arc::new(Shared { core, login }))
    }

    /// The identity provider the browser signs in with; `None` when the sign-in is not mounted.
    #[must_use]
    pub(crate) fn login_provider(&self) -> Option<&ProviderName> {
        self.0
            .login
            .as_ref()
            .map(|mounted| &mounted.settings.provider)
    }

    /// Answers a browser session request.
    pub(crate) async fn answer(
        &self,
        route: UiRoute,
        request: Request<ApiRequestBody>,
    ) -> Response<ApiResponseBody> {
        let (parts, _) = request.into_parts();
        let method = parts.method.clone();
        let client = parts
            .extensions
            .get::<ClientAddress>()
            .map(|address| address.0);
        let mut response = match route {
            UiRoute::Logout => {
                if method == Method::POST {
                    self.logout(&parts.headers)
                } else {
                    method_not_allowed("POST")
                }
            }
            UiRoute::Login => match &self.0.login {
                None => empty(StatusCode::NOT_FOUND),
                Some(mounted) if method == Method::GET => {
                    self.start(mounted, parts.uri.query(), client).await
                }
                Some(_) => method_not_allowed("GET"),
            },
            UiRoute::Callback => match &self.0.login {
                None => empty(StatusCode::NOT_FOUND),
                Some(mounted) if method == Method::GET => {
                    self.callback(mounted, parts.uri.query(), &parts.headers, client)
                        .await
                }
                Some(_) => method_not_allowed("GET"),
            },
        };
        if method == Method::HEAD {
            *response.body_mut() = full_body(Vec::new());
        }
        crate::responses::no_store(response.headers_mut());
        response
    }

    /// Ends the session when the request comes from the dashboard; any other request is refused
    /// before a cookie is cleared.
    fn logout(&self, headers: &HeaderMap) -> Response<ApiResponseBody> {
        let core = &self.0.core;
        if !is_same_origin_request(headers, core.host.endpoint.origin()) {
            return empty(StatusCode::FORBIDDEN);
        }
        match core.cookies.clear_session_header() {
            Ok(cookie) => {
                let mut response = empty(StatusCode::NO_CONTENT);
                response.headers_mut().append(SET_COOKIE, cookie);
                response
            }
            Err(error) => {
                tracing::error!(%error, "session cookie cannot be cleared");
                empty(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    /// The header that clears the sign-in cookie, or the failure when it cannot be built.
    fn login_clearing(&self) -> Result<HeaderValue, LoginFailure> {
        self.0
            .core
            .cookies
            .clear_login_header()
            .map_err(|_| LoginFailure::failed("sign-in cookie cannot be cleared"))
    }

    async fn start(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        query: Option<&str>,
        client: Option<IpAddr>,
    ) -> Response<ApiResponseBody> {
        if let Err(delay) = mounted.limiter.check(client) {
            return rate_limited(delay);
        }
        match self.begin(mounted, query).await {
            Ok(response) => response,
            Err(failure) => error_redirect(failure, Vec::new()),
        }
    }

    async fn begin(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        query: Option<&str>,
    ) -> Result<Response<ApiResponseBody>, LoginFailure> {
        let login = &mounted.settings;
        let core = &self.0.core;
        let requested = crate::query::query_param(query, "next");
        let next = safe_next(requested.as_deref());
        let challenge = mounted
            .verifier
            .begin_login(&LoginRequest {
                redirect_uri: core.host.endpoint.callback(),
                client: Some(&login.client),
            })
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let state = challenge.state;
        let location = HeaderValue::try_from(challenge.authorization_url.as_str())
            .map_err(|_| LoginFailure::failed("authorization url is not a header value"))?;
        let cookie = core
            .cookies
            .login_header(&state, login.ttl.get())
            .map_err(|_| LoginFailure::failed("sign-in cookie is not a header value"))?;
        mounted.attempts.lock().insert(
            state,
            Attempt {
                session: challenge.session,
                next,
            },
            core.host.clock.monotonic(),
        );
        Ok(redirect(location, vec![cookie]))
    }

    async fn callback(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        query: Option<&str>,
        headers: &HeaderMap,
        client: Option<IpAddr>,
    ) -> Response<ApiResponseBody> {
        if let Err(delay) = mounted.limiter.check(client) {
            return rate_limited(delay);
        }
        match self.finish(mounted, query, headers).await {
            Ok(response) => response,
            Err(failure) => error_redirect(failure, self.login_clearing().into_iter().collect()),
        }
    }

    async fn finish(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        query: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<Response<ApiResponseBody>, LoginFailure> {
        let login = &mounted.settings;
        let core = &self.0.core;
        let mut callback_url = core.host.endpoint.callback().clone();
        callback_url.set_query(query);
        let state = crate::query::query_param(query, "state");
        let clear = self.login_clearing()?;
        let cookie = cookie_value(headers, core.cookies.login);
        let (Some(state), Some(cookie)) = (state.as_deref(), cookie) else {
            return Err(LoginFailure::failed("sign-in state is missing"));
        };
        if state != cookie {
            return Err(LoginFailure::failed(
                "sign-in state does not match the sign-in cookie",
            ));
        }
        let attempt = mounted
            .attempts
            .lock()
            .take(state, core.host.clock.monotonic())
            .ok_or_else(|| LoginFailure::failed("sign-in attempt is unknown or expired"))?;
        let Attempt { session, next } = attempt;
        let grant = mounted
            .verifier
            .complete_login(session, &callback_url)
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let issued = core
            .clients
            .issue_session(
                &core.host,
                &TokenIssue {
                    provider: &login.provider,
                    proof: &grant.proof,
                },
            )
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let session_cookie = core
            .cookies
            .session_header(&issued.token, issued.expires_in)
            .map_err(|_| LoginFailure::failed("session cookie is not a header value"))?;
        Ok(redirect(next, vec![session_cookie, clear]))
    }
}

/// An empty rate-limit refusal with the rounded retry delay.
fn rate_limited(delay: std::time::Duration) -> Response<ApiResponseBody> {
    let mut response = empty(StatusCode::TOO_MANY_REQUESTS);
    response.headers_mut().insert(
        RETRY_AFTER,
        HeaderValue::from(crate::api_error::retry_seconds(delay)),
    );
    response
}

/// Why a sign-in failed. Only the reason reaches the browser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoginFailure {
    /// The identity provider's policy does not admit the caller, or its principal is revoked.
    Denied,
    /// The sign-in did not complete.
    Failed,
    /// A dependency of the sign-in is down.
    Unavailable,
}

impl LoginFailure {
    /// Classifies a port error from the sign-in and logs it.
    fn of(error: &PortError) -> Self {
        match error {
            PortError::Rejected {
                reason: Rejection::PolicyDenied | Rejection::PrincipalRevoked,
                ..
            } => {
                info!(error = %gang_types::error_chain(error), "browser sign-in denied");
                Self::Denied
            }
            PortError::Unavailable(_) => {
                warn!(error = %gang_types::error_chain(error), "browser sign-in unavailable");
                Self::Unavailable
            }
            _ => {
                warn!(error = %gang_types::error_chain(error), "browser sign-in failed");
                Self::Failed
            }
        }
    }

    /// A failure found without a port error, logged with `detail`.
    fn failed(detail: &'static str) -> Self {
        warn!(detail, "browser sign-in failed");
        Self::Failed
    }

    /// The reason the error page names.
    const fn reason(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::Failed => "failed",
            Self::Unavailable => "unavailable",
        }
    }
}

/// The safe dashboard path to redirect to, or `/ui` when `next` is unsafe.
fn safe_next(next: Option<&str>) -> HeaderValue {
    next.filter(|value| is_safe_next(value))
        .and_then(|value| HeaderValue::from_str(value).ok())
        .unwrap_or(HeaderValue::from_static(UI_HOME))
}

/// Whether `value` is a path under `/ui` that is safe to redirect to.
fn is_safe_next(value: &str) -> bool {
    let Some(rest) = value.strip_prefix(UI_HOME) else {
        return false;
    };
    let path = value.split_once('?').map_or(value, |(path, _)| path);
    matches!(rest.chars().next(), None | Some('/' | '?'))
        && !value.contains("//")
        && !value.contains('\\')
        && value.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
        && !path.split('/').any(is_dot_dot)
}

/// Whether a logout request comes from the dashboard: its `Sec-Fetch-Site` is `same-origin`, or
/// its `Origin` is exactly `endpoint_origin`.
fn is_same_origin_request(headers: &HeaderMap, endpoint_origin: &str) -> bool {
    let same_site = headers
        .get("sec-fetch-site")
        .is_some_and(|site| site == "same-origin");
    same_site
        || headers
            .get(ORIGIN)
            .is_some_and(|origin| origin == endpoint_origin)
}

/// Whether a path segment is `..`, with its dots possibly percent-encoded as `%2e` or `%2E`.
fn is_dot_dot(segment: &str) -> bool {
    segment == ".."
        || segment.eq_ignore_ascii_case(".%2e")
        || segment.eq_ignore_ascii_case("%2e.")
        || segment.eq_ignore_ascii_case("%2e%2e")
}

/// A `405` naming the methods the path accepts.
fn method_not_allowed(allow: &'static str) -> Response<ApiResponseBody> {
    let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
    response
        .headers_mut()
        .insert(ALLOW, HeaderValue::from_static(allow));
    response
}

/// A `302` to `location` that sets `set_cookies`.
fn redirect(location: HeaderValue, set_cookies: Vec<HeaderValue>) -> Response<ApiResponseBody> {
    let mut response = empty(StatusCode::FOUND);
    response.headers_mut().insert(LOCATION, location);
    for cookie in set_cookies {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
    response
}

/// A `302` to the error page, naming only the reason.
fn error_redirect(
    failure: LoginFailure,
    set_cookies: Vec<HeaderValue>,
) -> Response<ApiResponseBody> {
    let location = HeaderValue::from_str(&format!("{ERROR_PATH}?reason={}", failure.reason()))
        .unwrap_or_else(|_| HeaderValue::from_static(ERROR_PATH));
    redirect(location, set_cookies)
}

#[cfg(test)]
mod tests {
    use super::safe_next;

    #[test]
    fn safe_next_keeps_ui_paths_and_rejects_others() {
        for value in [
            "/ui",
            "/ui/",
            "/ui/workers/w1",
            "/ui?tab=1",
            "/ui/a?b=c",
            "/ui/a.b",
            "/ui/%2ea",
        ] {
            assert_eq!(safe_next(Some(value)), value);
        }
        for value in [
            "",
            "ui",
            "/",
            "/v1/state",
            "//evil.example",
            "/ui//x",
            "https://evil.example",
            "/ui\\x",
            "/uix",
            "/ui/../v1",
            "/ui/%2e%2e/v1",
            "/ui/.%2E/v1",
            "/ui/%2E./v1",
            "/ui/%2E%2e",
            "/ui\r\nSet-Cookie: x=y",
            "/ui\0",
        ] {
            assert_eq!(safe_next(Some(value)), "/ui", "{value:?}");
        }
        assert_eq!(safe_next(None), "/ui");
    }
}
